/*
 * clearprism_query.c — xBestIndex, xFilter, xNext, xEof, xColumn, xRowid
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

/* Internal helpers */
static int cursor_advance_source(clearprism_cursor *cur);
static int cursor_prepare_source(clearprism_cursor *cur, int source_idx,
                                  int argc, sqlite3_value **argv);
static void cursor_build_cache_key(clearprism_cursor *cur, int argc,
                                    sqlite3_value **argv, char *buf, int buf_size);
static void cursor_buffer_current_row(clearprism_cursor *cur);
static void cursor_flush_buffer_to_l1(clearprism_cursor *cur);
static void cursor_free_buffer(clearprism_cursor *cur);

int clearprism_vtab_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *info)
{
    clearprism_vtab *vtab = (clearprism_vtab *)pVtab;
    int flags = 0;

    char *idx_str = clearprism_where_encode(info, vtab->nCol, &flags);
    if (!idx_str) {
        idx_str = sqlite3_mprintf("");
    }

    /* Check for rowid equality lookup (iColumn == -1) */
    int rowid_argv = 0;
    for (int i = 0; i < info->nConstraint; i++) {
        if (info->aConstraint[i].iColumn == -1 &&
            info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ &&
            info->aConstraint[i].usable) {
            /* Find next available argvIndex */
            int max_argv = 0;
            for (int j = 0; j < info->nConstraint; j++) {
                if (info->aConstraintUsage[j].argvIndex > max_argv)
                    max_argv = info->aConstraintUsage[j].argvIndex;
            }
            info->aConstraintUsage[i].argvIndex = max_argv + 1;
            info->aConstraintUsage[i].omit = 1;
            flags |= CLEARPRISM_PLAN_ROWID_LOOKUP;
            info->estimatedCost = 1.0;
            info->estimatedRows = 1;
            rowid_argv = max_argv + 1;

            /* Append rowid constraint to idxStr: |-1:2 (col -1, op EQ=2) */
            int old_len = (int)strlen(idx_str);
            int new_size = old_len + 16;
            char *new_str = sqlite3_malloc(new_size);
            if (new_str) {
                if (old_len > 0)
                    snprintf(new_str, new_size, "%s|-1:2", idx_str);
                else
                    snprintf(new_str, new_size, "-1:2");
                sqlite3_free(idx_str);
                idx_str = new_str;
            }
            break;
        }
    }

    /* ORDER BY pushdown: only when single source is constrained */
    if ((flags & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) && info->nOrderBy > 0) {
        int can_order = 1;
        for (int i = 0; i < info->nOrderBy; i++) {
            int col = info->aOrderBy[i].iColumn;
            if (col < 0 || col >= vtab->nCol) { can_order = 0; break; }
        }
        if (can_order) {
            info->orderByConsumed = 1;
            flags |= CLEARPRISM_PLAN_HAS_ORDER;
            /* Append ORDER BY info after '#' separator */
            int old_len = (int)strlen(idx_str);
            int extra = info->nOrderBy * 16 + 2;
            char *new_str = sqlite3_malloc(old_len + extra);
            if (new_str) {
                int p = snprintf(new_str, old_len + extra, "%s#", idx_str);
                for (int i = 0; i < info->nOrderBy; i++) {
                    if (i > 0) new_str[p++] = '|';
                    p += snprintf(new_str + p, old_len + extra - p, "%d:%c",
                                  info->aOrderBy[i].iColumn,
                                  info->aOrderBy[i].desc ? 'D' : 'A');
                }
                sqlite3_free(idx_str);
                idx_str = new_str;
            }
        }
    }

    (void)rowid_argv;
    info->idxNum = flags;
    info->idxStr = idx_str;
    info->needToFreeIdxStr = 1;

    return SQLITE_OK;
}

int clearprism_vtab_filter(sqlite3_vtab_cursor *pCur, int idxNum,
                            const char *idxStr, int argc, sqlite3_value **argv)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;
    clearprism_vtab *vtab = cur->vtab;

    /* Clean up previous iteration state */
    if (cur->current_stmt) {
        sqlite3_finalize(cur->current_stmt);
        cur->current_stmt = NULL;
    }
    if (cur->current_conn && cur->sources &&
        cur->current_source_idx < cur->n_sources) {
        clearprism_connpool_checkin(vtab->pool,
                                    cur->sources[cur->current_source_idx].path);
        cur->current_conn = NULL;
    }
    clearprism_query_plan_clear(&cur->plan);
    clearprism_sources_free(cur->sources, cur->n_sources);
    cur->sources = NULL;
    cur->n_sources = 0;
    cur->current_source_idx = 0;
    cur->row_counter = 0;
    cur->eof = 0;
    cur->serving_from_cache = 0;

    /* Free saved argv from previous iteration */
    if (cur->saved_argv) {
        for (int i = 0; i < cur->saved_argc; i++) {
            sqlite3_value_free(cur->saved_argv[i]);
        }
        sqlite3_free(cur->saved_argv);
        cur->saved_argv = NULL;
        cur->saved_argc = 0;
    }

    if (cur->cache_cursor) {
        clearprism_cache_cursor_free(cur->cache_cursor);
        cur->cache_cursor = NULL;
    }

    /* Free previous IN expansion */
    if (cur->in_values) {
        for (int i = 0; i < cur->total_in_values; i++) {
            sqlite3_value_free(cur->in_values[i]);
        }
        sqlite3_free(cur->in_values);
        cur->in_values = NULL;
    }
    sqlite3_free(cur->in_offsets); cur->in_offsets = NULL;
    sqlite3_free(cur->in_counts); cur->in_counts = NULL;
    cur->n_in_constraints = 0;
    cur->total_in_values = 0;

    /* Decode plan */
    cur->plan.flags = idxNum;
    int rc = clearprism_where_decode(idxStr, &cur->plan);
    if (rc != SQLITE_OK) {
        cur->eof = 1;
        return rc;
    }

    /* If source-constrained, extract the source alias from argv */
    if (idxNum & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) {
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].col_idx == vtab->nCol &&
                cur->plan.constraints[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc) {
                    const char *alias = (const char *)sqlite3_value_text(argv[ai]);
                    if (alias) {
                        cur->plan.source_alias = clearprism_strdup(alias);
                    }
                }
                break;
            }
        }
    }

    /* Extract LIMIT value if present */
    cur->limit_remaining = -1;  /* no limit by default */
#ifdef SQLITE_INDEX_CONSTRAINT_LIMIT
    if (idxNum & CLEARPRISM_PLAN_HAS_LIMIT) {
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].op == SQLITE_INDEX_CONSTRAINT_LIMIT) {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc) {
                    cur->limit_remaining = sqlite3_value_int64(argv[ai]);
                    cur->plan.limit_value = cur->limit_remaining;
                }
                break;
            }
        }
    }
#endif

    /* Extract OFFSET value if present */
    cur->offset_remaining = 0;
#ifdef SQLITE_INDEX_CONSTRAINT_OFFSET
    if (idxNum & CLEARPRISM_PLAN_HAS_OFFSET) {
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].op == SQLITE_INDEX_CONSTRAINT_OFFSET) {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc) {
                    cur->offset_remaining = sqlite3_value_int64(argv[ai]);
                    cur->plan.offset_value = cur->offset_remaining;
                }
                break;
            }
        }
    }
#endif

    /* Expand IN constraints: iterate sqlite3_vtab_in_first/next, deep-copy values */
#if SQLITE_VERSION_NUMBER >= 3038000
    {
        /* Count IN constraints */
        int n_in = 0;
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].is_in) n_in++;
        }
        if (n_in > 0) {
            cur->n_in_constraints = n_in;
            cur->in_offsets = sqlite3_malloc(n_in * (int)sizeof(int));
            cur->in_counts = sqlite3_malloc(n_in * (int)sizeof(int));

            /* First pass: count values for each IN constraint */
            int in_idx = 0;
            int total = 0;
            for (int i = 0; i < cur->plan.n_constraints; i++) {
                if (!cur->plan.constraints[i].is_in) continue;
                int ai = cur->plan.constraints[i].argv_idx - 1;
                int count = 0;
                if (ai >= 0 && ai < argc) {
                    sqlite3_value *val = NULL;
                    int vrc = sqlite3_vtab_in_first(argv[ai], &val);
                    while (vrc == SQLITE_OK && val) {
                        count++;
                        vrc = sqlite3_vtab_in_next(argv[ai], &val);
                    }
                }
                if (cur->in_offsets) cur->in_offsets[in_idx] = total;
                if (cur->in_counts) cur->in_counts[in_idx] = count;
                cur->plan.constraints[i].in_count = count;
                cur->plan.constraints[i].in_offset = total;
                total += count;
                in_idx++;
            }
            cur->total_in_values = total;

            /* Allocate and deep-copy all IN values */
            if (total > 0) {
                cur->in_values = sqlite3_malloc(total * (int)sizeof(sqlite3_value *));
                if (cur->in_values) {
                    int vi = 0;
                    for (int i = 0; i < cur->plan.n_constraints; i++) {
                        if (!cur->plan.constraints[i].is_in) continue;
                        int ai = cur->plan.constraints[i].argv_idx - 1;
                        if (ai >= 0 && ai < argc) {
                            sqlite3_value *val = NULL;
                            int vrc = sqlite3_vtab_in_first(argv[ai], &val);
                            while (vrc == SQLITE_OK && val) {
                                cur->in_values[vi++] = sqlite3_value_dup(val);
                                vrc = sqlite3_vtab_in_next(argv[ai], &val);
                            }
                        }
                    }
                }
            }
        }
    }
#endif

    /* Save argv for re-binding on subsequent sources */
    if (argc > 0 && argv) {
        cur->saved_argc = argc;
        cur->saved_argv = sqlite3_malloc(argc * (int)sizeof(sqlite3_value *));
        if (cur->saved_argv) {
            for (int i = 0; i < argc; i++) {
                cur->saved_argv[i] = sqlite3_value_dup(argv[i]);
            }
        }
    }

    /* Build cache key and try cache lookup */
    char cache_key[512];
    cursor_build_cache_key(cur, argc, argv, cache_key, sizeof(cache_key));

    if (vtab->cache) {
        clearprism_cache_cursor *cc = NULL;
        int cache_hit = clearprism_cache_lookup(vtab->cache, cache_key, &cc);
        if (cache_hit && cc) {
            cur->cache_cursor = cc;
            cur->serving_from_cache = 1;
            if (clearprism_cache_cursor_eof(cc)) {
                cur->eof = 1;
            }
            return SQLITE_OK;
        }
    }

    /* Save cache key for L1 population after live query */
    cursor_free_buffer(cur);
    cur->cache_key = clearprism_strdup(cache_key);

    /* Rowid lookup fast-path: decode composite rowid, query single source */
    if (idxNum & CLEARPRISM_PLAN_ROWID_LOOKUP) {
        /* Find the rowid constraint argv index */
        int64_t composite = 0;
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].col_idx == -1 &&
                cur->plan.constraints[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc) {
                    composite = sqlite3_value_int64(argv[ai]);
                }
                break;
            }
        }
        int64_t target_source_id = composite >> CLEARPRISM_ROWID_SHIFT;
        int64_t target_rowid = composite & CLEARPRISM_ROWID_MASK;

        /* Get snapshot and find matching source */
        char *snap_err = NULL;
        rc = clearprism_registry_snapshot(vtab->registry, vtab->target_table,
                                           &cur->sources, &cur->n_sources, &snap_err);
        sqlite3_free(snap_err);

        int found = -1;
        for (int i = 0; i < cur->n_sources; i++) {
            if (cur->sources[i].id == target_source_id) { found = i; break; }
        }
        if (found < 0) {
            cur->eof = 1;
            return SQLITE_OK;
        }

        /* Checkout connection */
        char *pool_err = NULL;
        sqlite3 *conn = clearprism_connpool_checkout(vtab->pool,
                            cur->sources[found].path,
                            cur->sources[found].alias, &pool_err);
        sqlite3_free(pool_err);
        if (!conn) {
            cur->eof = 1;
            return SQLITE_OK;
        }
        cur->current_conn = conn;
        cur->current_source_idx = found;

        /* Build SELECT rowid, cols... FROM table WHERE rowid = ? */
        size_t sql_size = 256;
        for (int i = 0; i < vtab->nCol; i++)
            sql_size += strlen(vtab->cols[i].name) + 4;
        char *sql = sqlite3_malloc((int)sql_size);
        int p = snprintf(sql, sql_size, "SELECT rowid");
        for (int i = 0; i < vtab->nCol; i++)
            p += snprintf(sql + p, sql_size - p, ", \"%s\"", vtab->cols[i].name);
        p += snprintf(sql + p, sql_size - p, " FROM \"%s\" WHERE rowid = ?",
                       vtab->target_table);

        rc = sqlite3_prepare_v2(conn, sql, -1, &cur->current_stmt, NULL);
        sqlite3_free(sql);
        if (rc != SQLITE_OK) {
            clearprism_connpool_checkin(vtab->pool, cur->sources[found].path);
            cur->current_conn = NULL;
            cur->eof = 1;
            return SQLITE_OK;
        }
        sqlite3_bind_int64(cur->current_stmt, 1, target_rowid);
        rc = sqlite3_step(cur->current_stmt);
        if (rc == SQLITE_ROW) {
            cursor_buffer_current_row(cur);
        } else {
            cur->eof = 1;
        }
        return SQLITE_OK;
    }

    /* Get source snapshot */
    char *snap_err = NULL;
    rc = clearprism_registry_snapshot(vtab->registry, vtab->target_table,
                                       &cur->sources, &cur->n_sources, &snap_err);
    if (rc != SQLITE_OK) {
        clearprism_set_errmsg(&vtab->base, "%s", snap_err ? snap_err : "snapshot failed");
        sqlite3_free(snap_err);
        cur->eof = 1;
        return SQLITE_ERROR;
    }

    /* If source-constrained, filter to matching source */
    if (cur->plan.source_alias) {
        int found = 0;
        for (int i = 0; i < cur->n_sources; i++) {
            if (strcmp(cur->sources[i].alias, cur->plan.source_alias) == 0) {
                /* Swap to position 0 */
                if (i != 0) {
                    clearprism_source tmp = cur->sources[0];
                    cur->sources[0] = cur->sources[i];
                    cur->sources[i] = tmp;
                }
                /* Only iterate this one source */
                cur->n_sources = 1;
                found = 1;
                break;
            }
        }
        if (!found) {
            cur->n_sources = 0;
        }
    }

    if (cur->n_sources == 0 || cur->limit_remaining == 0) {
        cur->eof = 1;
        return SQLITE_OK;
    }

    /* Prepare first source */
    rc = cursor_prepare_source(cur, 0, argc, argv);
    if (rc != SQLITE_OK) {
        /* Try next source */
        cur->current_source_idx = 0;
        rc = cursor_advance_source(cur);
        if (rc != SQLITE_OK) {
            cur->eof = 1;
        }
    } else {
        /* Step to first row */
        rc = sqlite3_step(cur->current_stmt);
        if (rc == SQLITE_ROW) {
            cursor_buffer_current_row(cur);
        } else if (rc == SQLITE_DONE) {
            /* Empty source, try next */
            rc = cursor_advance_source(cur);
            if (rc != SQLITE_OK && rc != SQLITE_DONE) {
                cur->eof = 1;
            }
        } else {
            /* Error on first source, try next */
            rc = cursor_advance_source(cur);
            if (rc != SQLITE_OK && rc != SQLITE_DONE) {
                cur->eof = 1;
            }
        }
    }

    /* Skip OFFSET rows */
    while (cur->offset_remaining > 0 && !cur->eof) {
        cur->offset_remaining--;
        cur->row_counter++;
        /* Step like xNext but without LIMIT tracking */
        if (cur->current_stmt) {
            rc = sqlite3_step(cur->current_stmt);
            if (rc == SQLITE_ROW) {
                continue;
            }
            /* Source exhausted, advance */
            rc = cursor_advance_source(cur);
            if (rc != SQLITE_OK) {
                cur->eof = 1;
            }
        } else {
            cur->eof = 1;
        }
    }
    /* Buffer the first visible row if not eof */
    if (!cur->eof && cur->current_stmt && cur->offset_remaining == 0 &&
        cur->row_counter > 0) {
        cursor_buffer_current_row(cur);
    }

    return SQLITE_OK;
}

int clearprism_vtab_next(sqlite3_vtab_cursor *pCur)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;

    cur->row_counter++;

    /* Check LIMIT */
    if (cur->limit_remaining > 0) {
        cur->limit_remaining--;
        if (cur->limit_remaining == 0) {
            cur->eof = 1;
            cursor_flush_buffer_to_l1(cur);
            return SQLITE_OK;
        }
    }

    /* Cache serving path */
    if (cur->serving_from_cache && cur->cache_cursor) {
        clearprism_cache_cursor_next(cur->cache_cursor);
        if (clearprism_cache_cursor_eof(cur->cache_cursor)) {
            cur->eof = 1;
        }
        return SQLITE_OK;
    }

    /* Live query path */
    if (!cur->current_stmt) {
        cur->eof = 1;
        cursor_flush_buffer_to_l1(cur);
        return SQLITE_OK;
    }

    int rc = sqlite3_step(cur->current_stmt);
    if (rc == SQLITE_ROW) {
        cursor_buffer_current_row(cur);
        return SQLITE_OK;
    }

    /* Current source exhausted or errored — advance to next */
    rc = cursor_advance_source(cur);
    if (rc != SQLITE_OK) {
        cur->eof = 1;
        cursor_flush_buffer_to_l1(cur);
    }
    return SQLITE_OK;
}

int clearprism_vtab_eof(sqlite3_vtab_cursor *pCur)
{
    return ((clearprism_cursor *)pCur)->eof;
}

int clearprism_vtab_column(sqlite3_vtab_cursor *pCur, sqlite3_context *ctx,
                            int iCol)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;
    clearprism_vtab *vtab = cur->vtab;

    /* Cache serving path */
    if (cur->serving_from_cache && cur->cache_cursor) {
        sqlite3_value *val = clearprism_cache_cursor_value(cur->cache_cursor, iCol);
        if (val) {
            sqlite3_result_value(ctx, val);
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    /* Hidden _source_db column */
    if (iCol == vtab->nCol) {
        const char *alias = NULL;
        if (cur->current_source_idx < cur->n_sources) {
            alias = cur->sources[cur->current_source_idx].alias;
        }
        if (alias) {
            sqlite3_result_text(ctx, alias, -1, SQLITE_TRANSIENT);
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    /* Real columns — offset by CLEARPRISM_COL_OFFSET because rowid is column 0 */
    if (cur->current_stmt && iCol >= 0 && iCol < vtab->nCol) {
        sqlite3_result_value(ctx, sqlite3_column_value(cur->current_stmt,
                             iCol + CLEARPRISM_COL_OFFSET));
    } else {
        sqlite3_result_null(ctx);
    }

    return SQLITE_OK;
}

int clearprism_vtab_rowid(sqlite3_vtab_cursor *pCur, sqlite3_int64 *pRowid)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;

    if (cur->serving_from_cache) {
        /* Cache doesn't store source rowids — use monotonic counter */
        *pRowid = cur->row_counter;
        return SQLITE_OK;
    }

    if (cur->current_stmt && cur->current_source_idx < cur->n_sources) {
        int64_t source_id = cur->sources[cur->current_source_idx].id;
        int64_t source_rowid = sqlite3_column_int64(cur->current_stmt, 0);
        *pRowid = (source_id << CLEARPRISM_ROWID_SHIFT) |
                  (source_rowid & CLEARPRISM_ROWID_MASK);
    } else {
        *pRowid = cur->row_counter;
    }
    return SQLITE_OK;
}

/* ---------- Internal helpers ---------- */

/*
 * Advance to the next source that has rows.
 * Returns SQLITE_OK if a row is available, or SQLITE_DONE if all sources exhausted.
 */
static int cursor_advance_source(clearprism_cursor *cur)
{
    clearprism_vtab *vtab = cur->vtab;

    while (1) {
        /* Finalize current statement and check in connection */
        if (cur->current_stmt) {
            sqlite3_finalize(cur->current_stmt);
            cur->current_stmt = NULL;
        }
        if (cur->current_conn && cur->current_source_idx < cur->n_sources) {
            clearprism_connpool_checkin(vtab->pool,
                                        cur->sources[cur->current_source_idx].path);
            cur->current_conn = NULL;
        }

        cur->current_source_idx++;
        if (cur->current_source_idx >= cur->n_sources) {
            cur->eof = 1;
            return SQLITE_DONE;
        }

        /* Prepare next source with saved argv for parameter re-binding */
        int rc = cursor_prepare_source(cur, cur->current_source_idx,
                                        cur->saved_argc, cur->saved_argv);
        if (rc != SQLITE_OK) {
            /* Skip errored source, try next */
            continue;
        }

        rc = sqlite3_step(cur->current_stmt);
        if (rc == SQLITE_ROW) {
            return SQLITE_OK;
        } else if (rc == SQLITE_DONE) {
            /* Empty source, continue to next */
            continue;
        } else {
            /* Error, skip this source */
            continue;
        }
    }
}

/*
 * Prepare a SELECT statement for the given source index.
 * Binds constraint values from the plan.
 */
static int cursor_prepare_source(clearprism_cursor *cur, int source_idx,
                                  int argc, sqlite3_value **argv)
{
    clearprism_vtab *vtab = cur->vtab;

    if (source_idx >= cur->n_sources) return SQLITE_ERROR;

    /* Checkout connection from pool */
    char *pool_err = NULL;
    sqlite3 *conn = clearprism_connpool_checkout(vtab->pool,
                                                   cur->sources[source_idx].path,
                                                   cur->sources[source_idx].alias,
                                                   &pool_err);
    if (!conn) {
        sqlite3_free(pool_err);
        return SQLITE_ERROR;
    }
    cur->current_conn = conn;
    cur->current_source_idx = source_idx;

    /* Generate SQL */
    char *sql = clearprism_where_generate_sql(vtab->target_table,
                                               vtab->cols, vtab->nCol,
                                               &cur->plan);
    if (!sql) {
        clearprism_connpool_checkin(vtab->pool, cur->sources[source_idx].path);
        cur->current_conn = NULL;
        return SQLITE_NOMEM;
    }

    /* Prepare statement */
    int used_fallback = 0;
    int rc = sqlite3_prepare_v2(conn, sql, -1, &cur->current_stmt, NULL);
    sqlite3_free(sql);

    /* REGEXP/MATCH fallback: if prepare fails and plan has REGEXP/MATCH,
       retry without those constraints (SQLite will post-filter since omit=0) */
    if (rc != SQLITE_OK) {
        int has_regexp_match = 0;
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            int op = cur->plan.constraints[i].op;
            (void)op;
#ifdef SQLITE_INDEX_CONSTRAINT_REGEXP
            if (op == SQLITE_INDEX_CONSTRAINT_REGEXP) has_regexp_match = 1;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_MATCH
            if (op == SQLITE_INDEX_CONSTRAINT_MATCH) has_regexp_match = 1;
#endif
        }
        if (has_regexp_match) {
            /* Build a temporary plan excluding REGEXP/MATCH constraints */
            clearprism_query_plan fallback = cur->plan;
            int orig_n = cur->plan.n_constraints;
            clearprism_where_constraint *fb_cons = NULL;
            int fb_n = 0;
            if (orig_n > 0) {
                fb_cons = sqlite3_malloc(orig_n * (int)sizeof(*fb_cons));
                if (fb_cons) {
                    for (int i = 0; i < orig_n; i++) {
                        int fop = cur->plan.constraints[i].op;
                        int skip = 0;
                        (void)fop;
#ifdef SQLITE_INDEX_CONSTRAINT_REGEXP
                        if (fop == SQLITE_INDEX_CONSTRAINT_REGEXP) skip = 1;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_MATCH
                        if (fop == SQLITE_INDEX_CONSTRAINT_MATCH) skip = 1;
#endif
                        if (!skip) {
                            fb_cons[fb_n] = cur->plan.constraints[i];
                            fb_n++;
                        }
                    }
                }
            }
            fallback.constraints = fb_cons;
            fallback.n_constraints = fb_n;
            char *fb_sql = clearprism_where_generate_sql(vtab->target_table,
                                                          vtab->cols, vtab->nCol,
                                                          &fallback);
            sqlite3_free(fb_cons);
            if (fb_sql) {
                rc = sqlite3_prepare_v2(conn, fb_sql, -1, &cur->current_stmt, NULL);
                sqlite3_free(fb_sql);
                if (rc == SQLITE_OK) used_fallback = 1;
            }
        }
        if (rc != SQLITE_OK) {
            clearprism_connpool_checkin(vtab->pool, cur->sources[source_idx].path);
            cur->current_conn = NULL;
            return rc;
        }
    }

    /* Bind parameters from argv. We need to map plan constraints to bind positions.
       The SQL has ? placeholders for non-_source_db, non-unary constraints. */
    if (argc > 0 && argv) {
        int bind_idx = 1;
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            int col = cur->plan.constraints[i].col_idx;
            int op = cur->plan.constraints[i].op;
            /* Skip _source_db constraint — not in the SQL */
            if (col == vtab->nCol) continue;
            /* Skip rowid constraint — handled by rowid lookup path */
            if (col < 0) continue;
            if (col >= vtab->nCol) continue;

            /* Skip unary operators (IS NULL, IS NOT NULL) — no ? placeholder */
            int is_unary = 0;
#ifdef SQLITE_INDEX_CONSTRAINT_ISNULL
            if (op == SQLITE_INDEX_CONSTRAINT_ISNULL) is_unary = 1;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_ISNOTNULL
            if (op == SQLITE_INDEX_CONSTRAINT_ISNOTNULL) is_unary = 1;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_LIMIT
            if (op == SQLITE_INDEX_CONSTRAINT_LIMIT) continue;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_OFFSET
            if (op == SQLITE_INDEX_CONSTRAINT_OFFSET) continue;
#endif
            /* Skip REGEXP/MATCH when using fallback SQL (they're not in the SQL) */
            if (used_fallback) {
                int skip_rm = 0;
                (void)skip_rm;
#ifdef SQLITE_INDEX_CONSTRAINT_REGEXP
                if (op == SQLITE_INDEX_CONSTRAINT_REGEXP) skip_rm = 1;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_MATCH
                if (op == SQLITE_INDEX_CONSTRAINT_MATCH) skip_rm = 1;
#endif
                if (skip_rm) continue;
            }
            if (is_unary) continue;

            /* IN constraint: bind each expanded value */
            if (cur->plan.constraints[i].is_in && cur->in_values &&
                cur->plan.constraints[i].in_count > 0) {
                int off = cur->plan.constraints[i].in_offset;
                int cnt = cur->plan.constraints[i].in_count;
                for (int k = 0; k < cnt; k++) {
                    if (off + k < cur->total_in_values && cur->in_values[off + k]) {
                        sqlite3_bind_value(cur->current_stmt, bind_idx, cur->in_values[off + k]);
                    }
                    bind_idx++;
                }
            } else {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc) {
                    sqlite3_bind_value(cur->current_stmt, bind_idx, argv[ai]);
                }
                bind_idx++;
            }
        }
    }

    return SQLITE_OK;
}

static void cursor_build_cache_key(clearprism_cursor *cur, int argc,
                                    sqlite3_value **argv,
                                    char *buf, int buf_size)
{
    clearprism_vtab *vtab = cur->vtab;
    int pos = 0;

    pos += snprintf(buf + pos, buf_size - pos, "%s:", vtab->target_table);

    if (cur->plan.source_alias) {
        pos += snprintf(buf + pos, buf_size - pos, "src=%s:", cur->plan.source_alias);
    }

    /* Include constraint structure (col:op pairs) to differentiate queries
       with different WHERE clauses but identical parameter values */
    for (int i = 0; i < cur->plan.n_constraints; i++) {
        pos += snprintf(buf + pos, buf_size - pos, "c%d=%d:%d:",
                        i, cur->plan.constraints[i].col_idx,
                        cur->plan.constraints[i].op);
    }

    for (int i = 0; i < argc && i < cur->plan.n_constraints; i++) {
        int ai = cur->plan.constraints[i].argv_idx - 1;
        if (ai >= 0 && ai < argc && argv[ai]) {
            const char *txt = (const char *)sqlite3_value_text(argv[ai]);
            if (txt) {
                pos += snprintf(buf + pos, buf_size - pos, "p%d=%s:", i, txt);
            }
        }
    }
}

/*
 * Buffer the current row from the live statement into the L1 population buffer.
 * Skips if the buffer has overflowed (result set too large to cache).
 */
static void cursor_buffer_current_row(clearprism_cursor *cur)
{
    clearprism_vtab *vtab = cur->vtab;

    if (cur->buffer_overflow || !cur->cache_key || !vtab->cache) return;

    /* Check if buffering this row would exceed L1 limits */
    int n_values = vtab->nCol + 1;  /* real columns + _source_db */
    size_t row_overhead = sizeof(clearprism_l1_row) + n_values * sizeof(sqlite3_value *);

    if (cur->buffer_n_rows + 1 > vtab->l1_max_rows / 4) {
        /* Don't let a single query fill more than 25% of L1 */
        cur->buffer_overflow = 1;
        cursor_free_buffer(cur);
        return;
    }

    clearprism_l1_row *row = sqlite3_malloc(sizeof(*row));
    if (!row) { cur->buffer_overflow = 1; return; }
    memset(row, 0, sizeof(*row));

    row->n_values = n_values;
    row->values = sqlite3_malloc(n_values * (int)sizeof(sqlite3_value *));
    if (!row->values) {
        sqlite3_free(row);
        cur->buffer_overflow = 1;
        return;
    }

    size_t row_bytes = row_overhead;

    /* Copy real columns from statement — offset by COL_OFFSET (rowid is col 0) */
    for (int i = 0; i < vtab->nCol; i++) {
        sqlite3_value *src = sqlite3_column_value(cur->current_stmt,
                                                   i + CLEARPRISM_COL_OFFSET);
        row->values[i] = sqlite3_value_dup(src);
        row_bytes += clearprism_value_memsize(src);
    }

    /* Copy _source_db as the last value */
    const char *alias = NULL;
    if (cur->current_source_idx < cur->n_sources) {
        alias = cur->sources[cur->current_source_idx].alias;
    }
    if (alias) {
        /* Create a text value for the alias using a temporary statement */
        sqlite3 *mem_db = NULL;
        sqlite3_open(":memory:", &mem_db);
        if (mem_db) {
            sqlite3_stmt *tmp = NULL;
            sqlite3_prepare_v2(mem_db, "SELECT ?", -1, &tmp, NULL);
            if (tmp) {
                sqlite3_bind_text(tmp, 1, alias, -1, SQLITE_TRANSIENT);
                sqlite3_step(tmp);
                row->values[vtab->nCol] = sqlite3_value_dup(sqlite3_column_value(tmp, 0));
                row_bytes += strlen(alias) + 1;
                sqlite3_finalize(tmp);
            }
            sqlite3_close(mem_db);
        }
    } else {
        row->values[vtab->nCol] = NULL;
    }

    row->next = NULL;

    /* Append to buffer */
    if (cur->buffer_tail) {
        cur->buffer_tail->next = row;
    } else {
        cur->buffer_head = row;
    }
    cur->buffer_tail = row;
    cur->buffer_n_rows++;
    cur->buffer_bytes += row_bytes;
}

/*
 * Flush the buffered rows into L1 cache.
 */
static void cursor_flush_buffer_to_l1(clearprism_cursor *cur)
{
    if (!cur->cache_key || !cur->vtab->cache || cur->buffer_overflow ||
        cur->buffer_n_rows == 0 || !cur->buffer_head) {
        cursor_free_buffer(cur);
        return;
    }

    /* Transfer ownership of the buffer to L1 — don't free the rows here */
    clearprism_cache_store_l1(cur->vtab->cache, cur->cache_key,
                               cur->buffer_head, cur->buffer_n_rows,
                               cur->buffer_bytes);
    /* L1 now owns the rows */
    cur->buffer_head = NULL;
    cur->buffer_tail = NULL;
    cur->buffer_n_rows = 0;
    cur->buffer_bytes = 0;
    sqlite3_free(cur->cache_key);
    cur->cache_key = NULL;
}

/*
 * Free the row buffer without storing to cache.
 */
static void cursor_free_buffer(clearprism_cursor *cur)
{
    clearprism_l1_row *row = cur->buffer_head;
    while (row) {
        clearprism_l1_row *next = row->next;
        if (row->values) {
            for (int i = 0; i < row->n_values; i++) {
                sqlite3_value_free(row->values[i]);
            }
            sqlite3_free(row->values);
        }
        sqlite3_free(row);
        row = next;
    }
    cur->buffer_head = NULL;
    cur->buffer_tail = NULL;
    cur->buffer_n_rows = 0;
    cur->buffer_bytes = 0;
    sqlite3_free(cur->cache_key);
    cur->cache_key = NULL;
    cur->buffer_overflow = 0;
}
