/*
 * clearprism_registry.c — Registry DB reading, source enumeration
 */

#include <stdlib.h>
#include <string.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

clearprism_registry *clearprism_registry_open(const char *db_path, char **errmsg)
{
    if (!db_path) {
        if (errmsg) *errmsg = clearprism_strdup("registry path is NULL");
        return NULL;
    }

    clearprism_registry *reg = sqlite3_malloc(sizeof(*reg));
    if (!reg) {
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return NULL;
    }
    memset(reg, 0, sizeof(*reg));
    pthread_mutex_init(&reg->lock, NULL);

    reg->db_path = clearprism_strdup(db_path);
    if (!reg->db_path) {
        sqlite3_free(reg);
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return NULL;
    }

    int rc = sqlite3_open_v2(db_path, &reg->db,
                              SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                              NULL);
    if (rc != SQLITE_OK) {
        if (errmsg) {
            *errmsg = clearprism_mprintf("cannot open registry '%s': %s",
                                          db_path, sqlite3_errmsg(reg->db));
        }
        sqlite3_close(reg->db);
        sqlite3_free(reg->db_path);
        sqlite3_free(reg);
        return NULL;
    }

    rc = clearprism_registry_reload(reg, errmsg);
    if (rc != SQLITE_OK) {
        sqlite3_close(reg->db);
        sqlite3_free(reg->db_path);
        sqlite3_free(reg);
        return NULL;
    }

    return reg;
}

void clearprism_registry_close(clearprism_registry *reg)
{
    if (!reg) return;
    pthread_mutex_lock(&reg->lock);
    clearprism_sources_free(reg->sources, reg->n_sources);
    reg->sources = NULL;
    reg->n_sources = 0;
    sqlite3_close(reg->db);
    reg->db = NULL;
    sqlite3_free(reg->db_path);
    pthread_mutex_unlock(&reg->lock);
    pthread_mutex_destroy(&reg->lock);
    sqlite3_free(reg);
}

int clearprism_registry_reload(clearprism_registry *reg, char **errmsg)
{
    if (!reg || !reg->db) {
        if (errmsg) *errmsg = clearprism_strdup("registry not open");
        return SQLITE_ERROR;
    }

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT id, path, alias, active, priority "
        "FROM clearprism_sources "
        "WHERE active = 1 "
        "ORDER BY priority ASC, id ASC";

    int rc = sqlite3_prepare_v2(reg->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        if (errmsg) {
            *errmsg = clearprism_mprintf("registry query failed: %s",
                                          sqlite3_errmsg(reg->db));
        }
        return rc;
    }

    /* Count rows first by collecting into a dynamic array */
    int capacity = 16;
    int count = 0;
    clearprism_source *sources = sqlite3_malloc(capacity * (int)sizeof(*sources));
    if (!sources) {
        sqlite3_finalize(stmt);
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return SQLITE_NOMEM;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (count >= capacity) {
            capacity *= 2;
            clearprism_source *tmp = sqlite3_realloc(sources,
                                                      capacity * (int)sizeof(*sources));
            if (!tmp) {
                clearprism_sources_free(sources, count);
                sqlite3_finalize(stmt);
                if (errmsg) *errmsg = clearprism_strdup("out of memory");
                return SQLITE_NOMEM;
            }
            sources = tmp;
        }
        clearprism_source *s = &sources[count];
        s->id = sqlite3_column_int64(stmt, 0);
        s->path = clearprism_strdup((const char *)sqlite3_column_text(stmt, 1));
        s->alias = clearprism_strdup((const char *)sqlite3_column_text(stmt, 2));
        s->active = sqlite3_column_int(stmt, 3);
        s->priority = sqlite3_column_int(stmt, 4);
        count++;
    }
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        clearprism_sources_free(sources, count);
        if (errmsg) *errmsg = clearprism_mprintf("registry iteration error");
        return SQLITE_ERROR;
    }

    pthread_mutex_lock(&reg->lock);
    clearprism_sources_free(reg->sources, reg->n_sources);
    reg->sources = sources;
    reg->n_sources = count;
    pthread_mutex_unlock(&reg->lock);

    return SQLITE_OK;
}

int clearprism_registry_snapshot(clearprism_registry *reg,
                                  const char *table_name,
                                  clearprism_source **out_sources,
                                  int *out_n, char **errmsg)
{
    if (!reg) {
        if (errmsg) *errmsg = clearprism_strdup("registry is NULL");
        return SQLITE_ERROR;
    }

    pthread_mutex_lock(&reg->lock);

    /* If table_name is set, check for table_overrides that disable specific sources */
    int *excluded = NULL;
    int n_excluded = 0;

    if (table_name && reg->db) {
        sqlite3_stmt *stmt = NULL;
        const char *sql =
            "SELECT source_id FROM clearprism_table_overrides "
            "WHERE table_name = ? AND active = 0";
        int rc = sqlite3_prepare_v2(reg->db, sql, -1, &stmt, NULL);
        if (rc == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, table_name, -1, SQLITE_TRANSIENT);
            int cap = 8;
            excluded = sqlite3_malloc(cap * (int)sizeof(int));
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                if (n_excluded >= cap) {
                    cap *= 2;
                    excluded = sqlite3_realloc(excluded, cap * (int)sizeof(int));
                }
                if (excluded) {
                    excluded[n_excluded++] = sqlite3_column_int(stmt, 0);
                }
            }
            sqlite3_finalize(stmt);
        }
        /* If the table doesn't exist yet, that's fine — no overrides */
    }

    /* Build snapshot, excluding disabled sources */
    clearprism_source *snap = sqlite3_malloc(reg->n_sources * (int)sizeof(*snap));
    if (!snap) {
        pthread_mutex_unlock(&reg->lock);
        sqlite3_free(excluded);
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return SQLITE_NOMEM;
    }

    int count = 0;
    for (int i = 0; i < reg->n_sources; i++) {
        int skip = 0;
        for (int j = 0; j < n_excluded; j++) {
            if (reg->sources[i].id == excluded[j]) {
                skip = 1;
                break;
            }
        }
        if (skip) continue;

        snap[count].id = reg->sources[i].id;
        snap[count].path = clearprism_strdup(reg->sources[i].path);
        snap[count].alias = clearprism_strdup(reg->sources[i].alias);
        snap[count].active = reg->sources[i].active;
        snap[count].priority = reg->sources[i].priority;
        count++;
    }

    pthread_mutex_unlock(&reg->lock);
    sqlite3_free(excluded);

    *out_sources = snap;
    *out_n = count;
    return SQLITE_OK;
}

void clearprism_sources_free(clearprism_source *sources, int n)
{
    if (!sources) return;
    for (int i = 0; i < n; i++) {
        sqlite3_free(sources[i].path);
        sqlite3_free(sources[i].alias);
    }
    sqlite3_free(sources);
}
