/*
 * clearprism_scanner.c — Streaming scanner API for zero-vtab-overhead iteration
 *
 * Bypasses the SQLite virtual table protocol entirely, reading directly from
 * source databases via the connection pool.  Column accessors return pointers
 * into the source statement's memory (zero-copy, valid until the next
 * scan_next call).
 */

#if SQLITE_CORE
#include "sqlite3.h"
#else
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ========== Internal helpers ========== */

/*
 * Discover schema from the first available source.
 * Populates s->cols, s->n_cols.
 */
static int scanner_discover_schema(clearprism_scanner *s)
{
    for (int i = 0; i < s->n_sources; i++) {
        sqlite3 *db = NULL;
        int rc = sqlite3_open_v2(s->sources[i].path, &db,
                                 SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            continue;
        }

        char *pragma_sql = sqlite3_mprintf("PRAGMA table_info(\"%w\")",
                                           s->target_table);
        sqlite3_stmt *stmt = NULL;
        rc = sqlite3_prepare_v2(db, pragma_sql, -1, &stmt, NULL);
        sqlite3_free(pragma_sql);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            continue;
        }

        int cap = 16;
        s->cols = sqlite3_malloc(cap * (int)sizeof(clearprism_col_def));
        s->n_cols = 0;
        if (!s->cols) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return SQLITE_NOMEM;
        }

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (s->n_cols >= cap) {
                cap *= 2;
                clearprism_col_def *tmp = sqlite3_realloc(s->cols,
                    cap * (int)sizeof(clearprism_col_def));
                if (!tmp) {
                    sqlite3_finalize(stmt);
                    sqlite3_close(db);
                    return SQLITE_NOMEM;
                }
                s->cols = tmp;
            }
            clearprism_col_def *col = &s->cols[s->n_cols];
            col->name = clearprism_strdup(
                (const char *)sqlite3_column_text(stmt, 1));
            const char *type_str = (const char *)sqlite3_column_text(stmt, 2);
            col->type = type_str ? clearprism_strdup(type_str) : NULL;
            col->notnull = sqlite3_column_int(stmt, 3);
            col->pk = sqlite3_column_int(stmt, 5);
            s->n_cols++;
        }

        sqlite3_finalize(stmt);
        sqlite3_close(db);

        if (s->n_cols > 0)
            return SQLITE_OK;

        /* Table not found in this source — try next */
        for (int j = 0; j < s->n_cols; j++) {
            sqlite3_free(s->cols[j].name);
            sqlite3_free(s->cols[j].type);
        }
        sqlite3_free(s->cols);
        s->cols = NULL;
        s->n_cols = 0;
    }

    return SQLITE_ERROR;
}

/*
 * Build the base SQL: SELECT rowid, "col1", "col2", ... FROM "table"
 */
static int scanner_build_base_sql(clearprism_scanner *s)
{
    /* Calculate buffer size */
    int needed = 64 + (int)strlen(s->target_table);
    for (int i = 0; i < s->n_cols; i++)
        needed += (int)strlen(s->cols[i].name) + 4;  /* ", " + quotes */

    char *buf = sqlite3_malloc(needed);
    if (!buf) return SQLITE_NOMEM;

    int pos = 0;
    pos += snprintf(buf + pos, needed - pos, "SELECT rowid");
    for (int i = 0; i < s->n_cols; i++)
        pos += snprintf(buf + pos, needed - pos, ", \"%s\"", s->cols[i].name);
    pos += snprintf(buf + pos, needed - pos, " FROM \"%s\"", s->target_table);

    s->base_sql = buf;
    return SQLITE_OK;
}

/*
 * Build final SQL by appending WHERE clause if set.
 */
static void scanner_build_filter_sql(clearprism_scanner *s)
{
    sqlite3_free(s->filter_sql);
    if (s->where_expr) {
        s->filter_sql = sqlite3_mprintf("%s WHERE %s",
                                        s->base_sql, s->where_expr);
    } else {
        s->filter_sql = sqlite3_mprintf("%s", s->base_sql);
    }
}

/*
 * Clean up the current source connection + statement.
 */
static void scanner_cleanup_current(clearprism_scanner *s)
{
    if (s->current_stmt) {
        sqlite3_finalize(s->current_stmt);
        s->current_stmt = NULL;
    }
    if (s->current_conn && s->current_source >= 0 &&
        s->current_source < s->n_sources) {
        clearprism_connpool_checkin(s->pool,
            s->sources[s->current_source].path);
        s->current_conn = NULL;
    }
}

/*
 * Prepare and step the statement for source at index `idx`.
 * Returns 1 if a row is available, 0 if source is empty/errored.
 */
static int scanner_prepare_source(clearprism_scanner *s, int idx)
{
    char *errmsg = NULL;
    s->current_conn = clearprism_connpool_checkout(s->pool,
        s->sources[idx].path, s->sources[idx].alias, &errmsg);
    if (!s->current_conn) {
        sqlite3_free(errmsg);
        return 0;
    }

    const char *sql = s->filter_sql ? s->filter_sql : s->base_sql;
    int rc = sqlite3_prepare_v2(s->current_conn, sql, -1,
                                &s->current_stmt, NULL);
    if (rc != SQLITE_OK) {
        clearprism_connpool_checkin(s->pool, s->sources[idx].path);
        s->current_conn = NULL;
        return 0;
    }

    /* Bind parameters */
    for (int i = 0; i < s->n_binds; i++) {
        clearprism_scan_bind *b = &s->binds[i];
        switch (b->type) {
        case SQLITE_INTEGER:
            sqlite3_bind_int64(s->current_stmt, i + 1, b->u.i);
            break;
        case SQLITE_FLOAT:
            sqlite3_bind_double(s->current_stmt, i + 1, b->u.d);
            break;
        case SQLITE_TEXT:
            sqlite3_bind_text(s->current_stmt, i + 1,
                              b->text, b->text_len, SQLITE_STATIC);
            break;
        default:
            sqlite3_bind_null(s->current_stmt, i + 1);
            break;
        }
    }

    /* Step to first row */
    rc = sqlite3_step(s->current_stmt);
    if (rc == SQLITE_ROW) {
        s->current_source = idx;
        return 1;
    }

    /* Empty or error — clean up */
    sqlite3_finalize(s->current_stmt);
    s->current_stmt = NULL;
    clearprism_connpool_checkin(s->pool, s->sources[idx].path);
    s->current_conn = NULL;
    return 0;
}

/*
 * Advance to the next source that has rows.
 * Returns 1 if a source with rows was found, 0 if all exhausted.
 */
static int scanner_advance_source(clearprism_scanner *s)
{
    scanner_cleanup_current(s);

    for (int i = s->current_source + 1; i < s->n_sources; i++) {
        if (scanner_prepare_source(s, i))
            return 1;
    }

    return 0;
}

/* ========== Public API ========== */

clearprism_scanner *clearprism_scan_open(const char *registry_db,
                                          const char *table)
{
    if (!registry_db || !table) return NULL;

    clearprism_scanner *s = sqlite3_malloc(sizeof(*s));
    if (!s) return NULL;
    memset(s, 0, sizeof(*s));
    s->current_source = -1;

    /* Open registry */
    char *errmsg = NULL;
    s->registry = clearprism_registry_open(registry_db, &errmsg);
    if (!s->registry) {
        sqlite3_free(errmsg);
        sqlite3_free(s);
        return NULL;
    }
    s->owns_registry = 1;

    /* Snapshot sources */
    int rc = clearprism_registry_snapshot(s->registry, table,
                                          &s->sources, &s->n_sources, &errmsg);
    if (rc != SQLITE_OK || s->n_sources == 0) {
        sqlite3_free(errmsg);
        clearprism_registry_close(s->registry);
        sqlite3_free(s);
        return NULL;
    }

    s->target_table = clearprism_strdup(table);

    /* Discover schema from first available source */
    rc = scanner_discover_schema(s);
    if (rc != SQLITE_OK) {
        clearprism_sources_free(s->sources, s->n_sources);
        clearprism_registry_close(s->registry);
        sqlite3_free(s->target_table);
        sqlite3_free(s);
        return NULL;
    }

    /* Build base SQL */
    rc = scanner_build_base_sql(s);
    if (rc != SQLITE_OK) {
        for (int i = 0; i < s->n_cols; i++) {
            sqlite3_free(s->cols[i].name);
            sqlite3_free(s->cols[i].type);
        }
        sqlite3_free(s->cols);
        clearprism_sources_free(s->sources, s->n_sources);
        clearprism_registry_close(s->registry);
        sqlite3_free(s->target_table);
        sqlite3_free(s);
        return NULL;
    }

    /* Create connection pool */
    s->pool = clearprism_connpool_create(
        CLEARPRISM_DEFAULT_POOL_MAX_OPEN,
        CLEARPRISM_DEFAULT_POOL_TIMEOUT);
    if (!s->pool) {
        sqlite3_free(s->base_sql);
        for (int i = 0; i < s->n_cols; i++) {
            sqlite3_free(s->cols[i].name);
            sqlite3_free(s->cols[i].type);
        }
        sqlite3_free(s->cols);
        clearprism_sources_free(s->sources, s->n_sources);
        clearprism_registry_close(s->registry);
        sqlite3_free(s->target_table);
        sqlite3_free(s);
        return NULL;
    }
    s->owns_pool = 1;

    /* Build initial SQL (no filter yet) */
    scanner_build_filter_sql(s);

    return s;
}

int clearprism_scan_next(clearprism_scanner *s)
{
    if (!s || s->eof) return 0;

    /* First call — find first source with rows */
    if (!s->started) {
        s->started = 1;
        s->current_source = -1;
        for (int i = 0; i < s->n_sources; i++) {
            if (scanner_prepare_source(s, i))
                return 1;
        }
        s->eof = 1;
        return 0;
    }

    /* Step current statement */
    if (s->current_stmt) {
        int rc = sqlite3_step(s->current_stmt);
        if (rc == SQLITE_ROW)
            return 1;
    }

    /* Current source exhausted — advance */
    if (scanner_advance_source(s))
        return 1;

    s->eof = 1;
    return 0;
}

void clearprism_scan_close(clearprism_scanner *s)
{
    if (!s) return;

    scanner_cleanup_current(s);

    /* Free bind values */
    for (int i = 0; i < s->n_binds; i++)
        sqlite3_free(s->binds[i].text);

    /* Free SQL strings */
    sqlite3_free(s->base_sql);
    sqlite3_free(s->filter_sql);
    sqlite3_free(s->where_expr);

    /* Free schema */
    for (int i = 0; i < s->n_cols; i++) {
        sqlite3_free(s->cols[i].name);
        sqlite3_free(s->cols[i].type);
    }
    sqlite3_free(s->cols);

    /* Free sources */
    clearprism_sources_free(s->sources, s->n_sources);

    /* Free pool + registry if we own them */
    if (s->owns_pool)
        clearprism_connpool_destroy(s->pool);
    if (s->owns_registry)
        clearprism_registry_close(s->registry);

    sqlite3_free(s->target_table);
    sqlite3_free(s);
}

/* ========== Column accessors ========== */

int64_t clearprism_scan_int64(clearprism_scanner *s, int col)
{
    if (!s || !s->current_stmt) return 0;
    return sqlite3_column_int64(s->current_stmt, col + CLEARPRISM_COL_OFFSET);
}

double clearprism_scan_double(clearprism_scanner *s, int col)
{
    if (!s || !s->current_stmt) return 0.0;
    return sqlite3_column_double(s->current_stmt, col + CLEARPRISM_COL_OFFSET);
}

const char *clearprism_scan_text(clearprism_scanner *s, int col)
{
    if (!s || !s->current_stmt) return NULL;
    return (const char *)sqlite3_column_text(s->current_stmt,
                                             col + CLEARPRISM_COL_OFFSET);
}

const void *clearprism_scan_blob(clearprism_scanner *s, int col, int *len)
{
    if (!s || !s->current_stmt) {
        if (len) *len = 0;
        return NULL;
    }
    int sc = col + CLEARPRISM_COL_OFFSET;
    if (len) *len = sqlite3_column_bytes(s->current_stmt, sc);
    return sqlite3_column_blob(s->current_stmt, sc);
}

int clearprism_scan_type(clearprism_scanner *s, int col)
{
    if (!s || !s->current_stmt) return SQLITE_NULL;
    return sqlite3_column_type(s->current_stmt, col + CLEARPRISM_COL_OFFSET);
}

int clearprism_scan_is_null(clearprism_scanner *s, int col)
{
    return clearprism_scan_type(s, col) == SQLITE_NULL;
}

/* ========== Source identification ========== */

const char *clearprism_scan_source_alias(clearprism_scanner *s)
{
    if (!s || s->current_source < 0 || s->current_source >= s->n_sources)
        return NULL;
    return s->sources[s->current_source].alias;
}

int64_t clearprism_scan_source_id(clearprism_scanner *s)
{
    if (!s || s->current_source < 0 || s->current_source >= s->n_sources)
        return 0;
    return s->sources[s->current_source].id;
}

/* ========== Column metadata ========== */

int clearprism_scan_column_count(clearprism_scanner *s)
{
    return s ? s->n_cols : 0;
}

const char *clearprism_scan_column_name(clearprism_scanner *s, int col)
{
    if (!s || col < 0 || col >= s->n_cols) return NULL;
    return s->cols[col].name;
}

/* ========== Rowid and value accessors ========== */

int64_t clearprism_scan_rowid(clearprism_scanner *s)
{
    if (!s || !s->current_stmt) return 0;
    return sqlite3_column_int64(s->current_stmt, 0);  /* column 0 = rowid */
}

sqlite3_value *clearprism_scan_value(clearprism_scanner *s, int col)
{
    if (!s || !s->current_stmt) return NULL;
    return sqlite3_column_value(s->current_stmt, col + CLEARPRISM_COL_OFFSET);
}

/* ========== WHERE filtering ========== */

int clearprism_scan_filter(clearprism_scanner *s, const char *where_expr)
{
    if (!s || s->started) return SQLITE_MISUSE;

    sqlite3_free(s->where_expr);
    s->where_expr = clearprism_strdup(where_expr);

    /* Reset bind values */
    for (int i = 0; i < s->n_binds; i++)
        sqlite3_free(s->binds[i].text);
    s->n_binds = 0;

    /* Rebuild SQL */
    scanner_build_filter_sql(s);

    return SQLITE_OK;
}

int clearprism_scan_bind_int64(clearprism_scanner *s, int idx, int64_t val)
{
    if (!s || idx < 1 || idx > CLEARPRISM_SCAN_MAX_BINDS) return SQLITE_MISUSE;
    /* Expand n_binds to cover this index */
    while (s->n_binds < idx) {
        s->binds[s->n_binds].type = SQLITE_NULL;
        s->binds[s->n_binds].text = NULL;
        s->n_binds++;
    }
    clearprism_scan_bind *b = &s->binds[idx - 1];
    sqlite3_free(b->text);
    b->text = NULL;
    b->type = SQLITE_INTEGER;
    b->u.i = val;
    return SQLITE_OK;
}

int clearprism_scan_bind_double(clearprism_scanner *s, int idx, double val)
{
    if (!s || idx < 1 || idx > CLEARPRISM_SCAN_MAX_BINDS) return SQLITE_MISUSE;
    while (s->n_binds < idx) {
        s->binds[s->n_binds].type = SQLITE_NULL;
        s->binds[s->n_binds].text = NULL;
        s->n_binds++;
    }
    clearprism_scan_bind *b = &s->binds[idx - 1];
    sqlite3_free(b->text);
    b->text = NULL;
    b->type = SQLITE_FLOAT;
    b->u.d = val;
    return SQLITE_OK;
}

int clearprism_scan_bind_text(clearprism_scanner *s, int idx, const char *val)
{
    if (!s || idx < 1 || idx > CLEARPRISM_SCAN_MAX_BINDS) return SQLITE_MISUSE;
    while (s->n_binds < idx) {
        s->binds[s->n_binds].type = SQLITE_NULL;
        s->binds[s->n_binds].text = NULL;
        s->n_binds++;
    }
    clearprism_scan_bind *b = &s->binds[idx - 1];
    sqlite3_free(b->text);
    b->type = SQLITE_TEXT;
    b->text = clearprism_strdup(val);
    b->text_len = val ? (int)strlen(val) : 0;
    return SQLITE_OK;
}

int clearprism_scan_bind_null(clearprism_scanner *s, int idx)
{
    if (!s || idx < 1 || idx > CLEARPRISM_SCAN_MAX_BINDS) return SQLITE_MISUSE;
    while (s->n_binds < idx) {
        s->binds[s->n_binds].type = SQLITE_NULL;
        s->binds[s->n_binds].text = NULL;
        s->n_binds++;
    }
    clearprism_scan_bind *b = &s->binds[idx - 1];
    sqlite3_free(b->text);
    b->text = NULL;
    b->type = SQLITE_NULL;
    return SQLITE_OK;
}

/* ========== Callback iteration ========== */

int clearprism_scan_each(clearprism_scanner *s,
                          int (*callback)(clearprism_scanner *s, void *ctx),
                          void *ctx)
{
    if (!s || !callback) return SQLITE_MISUSE;
    while (clearprism_scan_next(s)) {
        int rc = callback(s, ctx);
        if (rc != 0) return rc;
    }
    return SQLITE_OK;
}
