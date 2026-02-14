/*
 * clearprism_query.c — xBestIndex, xFilter, xNext, xEof, xColumn, xRowid
 *
 * Sources are prepared in parallel using a thread pool, then iterated
 * sequentially (or via merge-sort heap for multi-source ORDER BY).
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
static int  cursor_advance_source(clearprism_cursor *cur);
static void handle_prepare_and_step(clearprism_cursor *cur,
                                     clearprism_source_handle *h);
static void cursor_cleanup_handles(clearprism_cursor *cur);
static void cursor_build_cache_key(clearprism_cursor *cur, int argc,
                                    sqlite3_value **argv, char *buf, int buf_size);
static void cursor_buffer_current_row(clearprism_cursor *cur);
static void cursor_flush_buffer_to_l1(clearprism_cursor *cur);
static void cursor_free_buffer(clearprism_cursor *cur);
static void cursor_precreate_alias_values(clearprism_cursor *cur);
static void cursor_free_alias_values(clearprism_cursor *cur);
static void cursor_free_drain(clearprism_cursor *cur);
static void cursor_flush_drain_to_l1(clearprism_cursor *cur);
static void *prefetch_worker(void *arg);

/* Merge-sort heap helpers */
static int  merge_compare(clearprism_cursor *cur, int a_idx, int b_idx);
static void heap_sift_down(clearprism_cursor *cur, int i);
static void heap_build(clearprism_cursor *cur);

/* Parallel prepare infrastructure */
struct parallel_prepare_ctx {
    clearprism_cursor *cur;
    int next_handle;  /* atomically incremented */
};

static void *parallel_prepare_worker(void *arg)
{
    struct parallel_prepare_ctx *ctx = (struct parallel_prepare_ctx *)arg;
    while (1) {
        int idx = __sync_fetch_and_add(&ctx->next_handle, 1);
        if (idx >= ctx->cur->n_handles) break;
        handle_prepare_and_step(ctx->cur, &ctx->cur->handles[idx]);
    }
    return NULL;
}

/* Background prefetch: prepare the next source handle while the current
 * one is being iterated.  This overlaps I/O (sqlite3_open + prepare) with
 * row iteration, hiding source-switch latency. */
static void *prefetch_worker(void *arg)
{
    clearprism_cursor *cur = (clearprism_cursor *)arg;
    int idx = cur->prefetch_next_idx;
    if (idx >= 0 && idx < cur->n_handles) {
        clearprism_source_handle *h = &cur->handles[idx];
        if (!h->stmt && !h->errored)
            handle_prepare_and_step(cur, h);
    }
    return NULL;
}

/* ========== Parallel drain infrastructure (Optimization 1) ========== */

struct drain_worker_ctx {
    clearprism_cursor *cur;
    int next_handle;              /* atomic counter */
    /* Per-handle output buffers */
    sqlite3_value ***per_handle_values;  /* [handle_idx] -> flat value array */
    int64_t        **per_handle_rowids;  /* [handle_idx] -> rowid array */
    int             *per_handle_counts;  /* [handle_idx] -> row count */
    int             *per_handle_caps;    /* [handle_idx] -> capacity */
    int              n_cols;
};

static void *drain_worker(void *arg)
{
    struct drain_worker_ctx *ctx = (struct drain_worker_ctx *)arg;
    clearprism_cursor *cur = ctx->cur;
    int n_cols = ctx->n_cols;

    while (1) {
        int idx = __sync_fetch_and_add(&ctx->next_handle, 1);
        if (idx >= cur->n_handles) break;

        clearprism_source_handle *h = &cur->handles[idx];
        if (!h->stmt || h->errored || !h->has_row) {
            ctx->per_handle_counts[idx] = 0;
            continue;
        }

        int si = h->source_idx;
        int64_t source_id = (si < cur->n_sources) ? cur->sources[si].id : 0;

        /* Start with the already-stepped first row, then drain remaining */
        int cap = 256;
        sqlite3_value **vals = sqlite3_malloc(cap * n_cols * (int)sizeof(sqlite3_value *));
        int64_t *rowids = sqlite3_malloc(cap * (int)sizeof(int64_t));
        if (!vals || !rowids) {
            sqlite3_free(vals);
            sqlite3_free(rowids);
            ctx->per_handle_counts[idx] = 0;
            continue;
        }

        int count = 0;
        int rc = SQLITE_ROW; /* first row already stepped */

        while (rc == SQLITE_ROW) {
            /* Grow buffers if needed */
            if (count >= cap) {
                cap *= 2;
                vals = sqlite3_realloc(vals, cap * n_cols * (int)sizeof(sqlite3_value *));
                rowids = sqlite3_realloc(rowids, cap * (int)sizeof(int64_t));
                if (!vals || !rowids) break;
            }

            /* Copy columns */
            sqlite3_value **row_vals = &vals[count * n_cols];
            for (int c = 0; c < cur->vtab->nCol; c++) {
                sqlite3_value *src = sqlite3_column_value(h->stmt, c + CLEARPRISM_COL_OFFSET);
                row_vals[c] = sqlite3_value_dup(src);
            }
            /* Copy _source_db alias as last column */
            if (si < cur->n_sources && cur->alias_values && cur->alias_values[si])
                row_vals[cur->vtab->nCol] = sqlite3_value_dup(cur->alias_values[si]);
            else
                row_vals[cur->vtab->nCol] = NULL;

            /* Compute composite rowid */
            int64_t source_rowid = sqlite3_column_int64(h->stmt, 0);
            rowids[count] = (source_id << CLEARPRISM_ROWID_SHIFT) |
                            (source_rowid & CLEARPRISM_ROWID_MASK);

            count++;

            /* Step to next row */
            rc = sqlite3_step(h->stmt);
        }

        ctx->per_handle_values[idx] = vals;
        ctx->per_handle_rowids[idx] = rowids;
        ctx->per_handle_counts[idx] = count;
        ctx->per_handle_caps[idx] = cap;
    }
    return NULL;
}

/*
 * Attempt parallel drain of all source handles into a flat materialized buffer.
 * Returns 1 if drain succeeded and cursor should serve from drain, 0 otherwise.
 */
static int cursor_try_parallel_drain(clearprism_cursor *cur, int idxNum)
{
    clearprism_vtab *vtab = cur->vtab;
    int n_cols = vtab->nCol + 1;  /* +1 for _source_db */

    /* Multi-source ORDER BY: the live merge-sort heap is far more efficient
     * than drain.  The heap only processes LIMIT rows via xNext, while drain
     * would materialize ALL rows from all sources upfront.  For ORDER BY
     * LIMIT 100 across 20 sources, this avoids materializing 2000 rows. */
    if (idxNum & CLEARPRISM_PLAN_HAS_MERGE_ORDER) return 0;

    /* Check if drain should be attempted based on query plan heuristics */
    int should_drain = 0;
    if (idxNum & CLEARPRISM_PLAN_HAS_WHERE) {
        should_drain = 1;  /* Selective query */
    } else if (idxNum & CLEARPRISM_PLAN_HAS_LIMIT) {
        should_drain = 1;  /* Bounded result */
    } else if (idxNum & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) {
        should_drain = 1;  /* Single source */
    } else if (cur->n_handles <= 1) {
        should_drain = 0;  /* Single source, sequential is fine */
    }
    /* Full scan on many sources: skip drain */
    if (!should_drain) return 0;

    /* Count handles that have rows */
    int active_handles = 0;
    for (int i = 0; i < cur->n_handles; i++) {
        if (cur->handles[i].has_row && !cur->handles[i].errored)
            active_handles++;
    }
    if (active_handles == 0) return 0;

    /* Set up drain context */
    struct drain_worker_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cur = cur;
    ctx.next_handle = 0;
    ctx.n_cols = n_cols;
    ctx.per_handle_values = sqlite3_malloc(cur->n_handles * (int)sizeof(sqlite3_value **));
    ctx.per_handle_rowids = sqlite3_malloc(cur->n_handles * (int)sizeof(int64_t *));
    ctx.per_handle_counts = sqlite3_malloc(cur->n_handles * (int)sizeof(int));
    ctx.per_handle_caps = sqlite3_malloc(cur->n_handles * (int)sizeof(int));

    if (!ctx.per_handle_values || !ctx.per_handle_rowids ||
        !ctx.per_handle_counts || !ctx.per_handle_caps) {
        sqlite3_free(ctx.per_handle_values);
        sqlite3_free(ctx.per_handle_rowids);
        sqlite3_free(ctx.per_handle_counts);
        sqlite3_free(ctx.per_handle_caps);
        return 0;
    }
    memset(ctx.per_handle_values, 0, cur->n_handles * (int)sizeof(sqlite3_value **));
    memset(ctx.per_handle_rowids, 0, cur->n_handles * (int)sizeof(int64_t *));
    memset(ctx.per_handle_counts, 0, cur->n_handles * (int)sizeof(int));
    memset(ctx.per_handle_caps, 0, cur->n_handles * (int)sizeof(int));

    /* Launch drain workers */
    int n_threads = active_handles;
    if (n_threads > CLEARPRISM_MAX_PREPARE_THREADS)
        n_threads = CLEARPRISM_MAX_PREPARE_THREADS;
    if (active_handles < CLEARPRISM_MIN_PARALLEL_SOURCES)
        n_threads = 1;
    if (n_threads < 1) n_threads = 1;

    if (n_threads == 1) {
        drain_worker(&ctx);
    } else {
        pthread_t *threads = sqlite3_malloc(n_threads * (int)sizeof(pthread_t));
        if (threads) {
            for (int i = 0; i < n_threads; i++)
                pthread_create(&threads[i], NULL, drain_worker, &ctx);
            for (int i = 0; i < n_threads; i++)
                pthread_join(threads[i], NULL);
            sqlite3_free(threads);
        } else {
            drain_worker(&ctx);
        }
    }

    /* Compute total row count */
    int total_rows = 0;
    for (int i = 0; i < cur->n_handles; i++)
        total_rows += ctx.per_handle_counts[i];

    if (total_rows == 0) {
        sqlite3_free(ctx.per_handle_values);
        sqlite3_free(ctx.per_handle_rowids);
        sqlite3_free(ctx.per_handle_counts);
        sqlite3_free(ctx.per_handle_caps);
        return 0;
    }

    /* Merge phase: concatenate per-handle buffers into single flat array */
    sqlite3_value **merged_values = sqlite3_malloc(total_rows * n_cols * (int)sizeof(sqlite3_value *));
    clearprism_l1_row *merged_rows = sqlite3_malloc(total_rows * (int)sizeof(clearprism_l1_row));

    if (!merged_values || !merged_rows) {
        sqlite3_free(merged_values);
        sqlite3_free(merged_rows);
        for (int i = 0; i < cur->n_handles; i++) {
            if (ctx.per_handle_values[i]) {
                int cnt = ctx.per_handle_counts[i];
                for (int r = 0; r < cnt * n_cols; r++)
                    sqlite3_value_free(ctx.per_handle_values[i][r]);
                sqlite3_free(ctx.per_handle_values[i]);
            }
            sqlite3_free(ctx.per_handle_rowids[i]);
        }
        sqlite3_free(ctx.per_handle_values);
        sqlite3_free(ctx.per_handle_rowids);
        sqlite3_free(ctx.per_handle_counts);
        sqlite3_free(ctx.per_handle_caps);
        return 0;
    }

    if (idxNum & CLEARPRISM_PLAN_HAS_MERGE_ORDER) {
        /* Heap-merge: maintain sorted order across sources */
        /* Build per-handle cursor positions */
        int *h_pos = sqlite3_malloc(cur->n_handles * (int)sizeof(int));
        if (!h_pos) goto merge_fallback;
        memset(h_pos, 0, cur->n_handles * (int)sizeof(int));

        /* Build initial min-heap of handle indices (handles with rows) */
        int *h_heap = sqlite3_malloc(cur->n_handles * (int)sizeof(int));
        int h_heap_size = 0;
        if (!h_heap) { sqlite3_free(h_pos); goto merge_fallback; }
        for (int i = 0; i < cur->n_handles; i++) {
            if (ctx.per_handle_counts[i] > 0)
                h_heap[h_heap_size++] = i;
        }

        /* Compare function for drain merge: compare values at current positions */
        /* We'll use inline heap operations since we can't use merge_compare
         * (that works on stmt columns, not materialized values) */
        #define DRAIN_VAL(hi, col) \
            ctx.per_handle_values[hi][h_pos[hi] * n_cols + (col)]

        /* Sift-down for drain merge heap */
        #define DRAIN_CMP(a_hi, b_hi) drain_merge_cmp(cur, &ctx, h_pos, n_cols, a_hi, b_hi)

        /* We need a helper inline — define it as a local function via goto-free approach */
        int out_idx = 0;
        /* Simple inline heap-merge using insertion — build heap first */
        /* Floyd's heap build */
        for (int i = h_heap_size / 2 - 1; i >= 0; i--) {
            /* Sift down position i */
            int pos = i;
            while (1) {
                int smallest = pos;
                int left = 2 * pos + 1, right = 2 * pos + 2;
                if (left < h_heap_size) {
                    int a = h_heap[left], b = h_heap[smallest];
                    /* Compare current rows of handles a and b */
                    int cmp = 0;
                    for (int oi = 0; oi < cur->plan.n_order_cols && cmp == 0; oi++) {
                        int col = cur->plan.order_cols[oi].col_idx;
                        sqlite3_value *va = ctx.per_handle_values[a][h_pos[a] * n_cols + col];
                        sqlite3_value *vb = ctx.per_handle_values[b][h_pos[b] * n_cols + col];
                        cmp = clearprism_value_compare(va, vb);
                        if (cur->plan.order_cols[oi].desc) cmp = -cmp;
                    }
                    if (cmp == 0) cmp = a - b;
                    if (cmp < 0) smallest = left;
                }
                if (right < h_heap_size) {
                    int a = h_heap[right], b = h_heap[smallest];
                    int cmp = 0;
                    for (int oi = 0; oi < cur->plan.n_order_cols && cmp == 0; oi++) {
                        int col = cur->plan.order_cols[oi].col_idx;
                        sqlite3_value *va = ctx.per_handle_values[a][h_pos[a] * n_cols + col];
                        sqlite3_value *vb = ctx.per_handle_values[b][h_pos[b] * n_cols + col];
                        cmp = clearprism_value_compare(va, vb);
                        if (cur->plan.order_cols[oi].desc) cmp = -cmp;
                    }
                    if (cmp == 0) cmp = a - b;
                    if (cmp < 0) smallest = right;
                }
                if (smallest == pos) break;
                int tmp = h_heap[pos]; h_heap[pos] = h_heap[smallest]; h_heap[smallest] = tmp;
                pos = smallest;
            }
        }

        /* Pop min from heap, copy to output, advance that handle's cursor */
        while (h_heap_size > 0) {
            int hi = h_heap[0];
            int rpos = h_pos[hi];

            /* Copy values (transfer ownership — no dup needed) */
            memcpy(&merged_values[out_idx * n_cols],
                   &ctx.per_handle_values[hi][rpos * n_cols],
                   n_cols * sizeof(sqlite3_value *));
            merged_rows[out_idx].composite_rowid = ctx.per_handle_rowids[hi][rpos];
            merged_rows[out_idx].values = &merged_values[out_idx * n_cols];
            merged_rows[out_idx].n_values = n_cols;
            merged_rows[out_idx].next = NULL;
            out_idx++;

            /* Advance this handle's position */
            h_pos[hi]++;
            if (h_pos[hi] >= ctx.per_handle_counts[hi]) {
                /* Handle exhausted — remove from heap */
                h_heap[0] = h_heap[--h_heap_size];
            }

            /* Sift down position 0 */
            if (h_heap_size > 0) {
                int pos = 0;
                while (1) {
                    int smallest = pos;
                    int left = 2 * pos + 1, right = 2 * pos + 2;
                    if (left < h_heap_size) {
                        int a = h_heap[left], b = h_heap[smallest];
                        int cmp = 0;
                        for (int oi = 0; oi < cur->plan.n_order_cols && cmp == 0; oi++) {
                            int col = cur->plan.order_cols[oi].col_idx;
                            sqlite3_value *va = ctx.per_handle_values[a][h_pos[a] * n_cols + col];
                            sqlite3_value *vb = ctx.per_handle_values[b][h_pos[b] * n_cols + col];
                            cmp = clearprism_value_compare(va, vb);
                            if (cur->plan.order_cols[oi].desc) cmp = -cmp;
                        }
                        if (cmp == 0) cmp = a - b;
                        if (cmp < 0) smallest = left;
                    }
                    if (right < h_heap_size) {
                        int a = h_heap[right], b = h_heap[smallest];
                        int cmp = 0;
                        for (int oi = 0; oi < cur->plan.n_order_cols && cmp == 0; oi++) {
                            int col = cur->plan.order_cols[oi].col_idx;
                            sqlite3_value *va = ctx.per_handle_values[a][h_pos[a] * n_cols + col];
                            sqlite3_value *vb = ctx.per_handle_values[b][h_pos[b] * n_cols + col];
                            cmp = clearprism_value_compare(va, vb);
                            if (cur->plan.order_cols[oi].desc) cmp = -cmp;
                        }
                        if (cmp == 0) cmp = a - b;
                        if (cmp < 0) smallest = right;
                    }
                    if (smallest == pos) break;
                    int tmp = h_heap[pos]; h_heap[pos] = h_heap[smallest]; h_heap[smallest] = tmp;
                    pos = smallest;
                }
            }
        }
        #undef DRAIN_VAL
        #undef DRAIN_CMP

        sqlite3_free(h_pos);
        sqlite3_free(h_heap);

        /* Null out per-handle value pointers (ownership transferred to merged) */
        for (int i = 0; i < cur->n_handles; i++) {
            sqlite3_free(ctx.per_handle_values[i]); /* free the array shell */
            ctx.per_handle_values[i] = NULL;
        }
        goto drain_done;

    merge_fallback:
        (void)0;  /* Fall through to concatenation */
    }

    /* No ORDER BY or merge fallback: simple concatenation */
    {
        int out_idx = 0;
        /* For LIMIT queries, only concatenate enough rows */
        int want = total_rows;
        if (cur->limit_remaining > 0) {
            int64_t lim = cur->limit_remaining;
            if (cur->offset_remaining > 0) lim += cur->offset_remaining;
            if (lim < total_rows) want = (int)lim;
        }
        for (int i = 0; i < cur->n_handles && out_idx < want; i++) {
            int cnt = ctx.per_handle_counts[i];
            if (cnt == 0) continue;
            int to_copy = cnt;
            if (out_idx + to_copy > want) to_copy = want - out_idx;
            /* Transfer values (move pointers, no dup) */
            memcpy(&merged_values[out_idx * n_cols],
                   ctx.per_handle_values[i],
                   to_copy * n_cols * (int)sizeof(sqlite3_value *));
            for (int r = 0; r < to_copy; r++) {
                merged_rows[out_idx + r].composite_rowid = ctx.per_handle_rowids[i][r];
                merged_rows[out_idx + r].values = &merged_values[(out_idx + r) * n_cols];
                merged_rows[out_idx + r].n_values = n_cols;
                merged_rows[out_idx + r].next = NULL;
            }
            out_idx += to_copy;
            /* Free excess values beyond what was copied */
            if (to_copy < cnt) {
                for (int r = to_copy; r < cnt; r++)
                    for (int c = 0; c < n_cols; c++)
                        sqlite3_value_free(ctx.per_handle_values[i][r * n_cols + c]);
            }
            sqlite3_free(ctx.per_handle_values[i]);
            ctx.per_handle_values[i] = NULL;
        }
        total_rows = out_idx;  /* Update for subsequent code */
    }

drain_done:
    /* Free per-handle tracking arrays */
    for (int i = 0; i < cur->n_handles; i++) {
        /* Any remaining non-transferred values (shouldn't happen but be safe) */
        if (ctx.per_handle_values[i]) {
            int cnt = ctx.per_handle_counts[i];
            for (int r = 0; r < cnt * n_cols; r++)
                sqlite3_value_free(ctx.per_handle_values[i][r]);
            sqlite3_free(ctx.per_handle_values[i]);
        }
        sqlite3_free(ctx.per_handle_rowids[i]);
    }
    sqlite3_free(ctx.per_handle_values);
    sqlite3_free(ctx.per_handle_rowids);
    sqlite3_free(ctx.per_handle_counts);
    sqlite3_free(ctx.per_handle_caps);

    /* Store drain result on cursor */
    cur->drain_rows = merged_rows;
    cur->drain_values = merged_values;
    cur->drain_n_rows = total_rows;
    cur->drain_n_cols = n_cols;
    cur->drain_idx = 0;
    cur->serving_from_drain = 1;

    /* Finalize all handles and checkin connections since we've drained everything */
    for (int i = 0; i < cur->n_handles; i++) {
        clearprism_source_handle *h = &cur->handles[i];
        if (h->stmt) { sqlite3_finalize(h->stmt); h->stmt = NULL; }
        if (h->conn && h->source_idx < cur->n_sources) {
            clearprism_connpool_checkin(vtab->pool, cur->sources[h->source_idx].path);
            h->conn = NULL;
        }
    }

    return 1;  /* Drain succeeded */
}

static void cursor_free_drain(clearprism_cursor *cur)
{
    cursor_flush_drain_to_l1(cur);
    if (cur->drain_values) {
        int total = cur->drain_n_rows * cur->drain_n_cols;
        for (int i = 0; i < total; i++)
            sqlite3_value_free(cur->drain_values[i]);
        sqlite3_free(cur->drain_values);
        cur->drain_values = NULL;
    }
    sqlite3_free(cur->drain_rows);
    cur->drain_rows = NULL;
    cur->drain_n_rows = 0;
    cur->drain_n_cols = 0;
    cur->drain_idx = 0;
    cur->serving_from_drain = 0;
}

/* Public wrapper: flush drain to L1 then free — called from xClose */
void clearprism_cursor_flush_drain(clearprism_cursor *cur)
{
    cursor_free_drain(cur);
}

/*
 * Transfer drain buffer ownership to L1 cache (zero-copy).
 * Avoids duplicating values that drain already materialized.
 */
static void cursor_flush_drain_to_l1(clearprism_cursor *cur)
{
    if (!cur->cache_key || !cur->vtab->cache ||
        !cur->drain_rows || !cur->drain_values || cur->drain_n_rows == 0)
        return;

    clearprism_vtab *vtab = cur->vtab;
    int n = cur->drain_n_rows;
    int nc = cur->drain_n_cols;

    if (n > (int)vtab->l1_max_rows) return;

    /* Calculate byte size for L1 budget check */
    size_t byte_size = 0;
    for (int r = 0; r < n; r++) {
        for (int c = 0; c < nc; c++) {
            sqlite3_value *v = cur->drain_values[r * nc + c];
            if (v) byte_size += clearprism_value_memsize(v);
        }
        byte_size += sizeof(clearprism_l1_row) + nc * sizeof(sqlite3_value *);
    }

    if ((int64_t)byte_size > vtab->l1_max_bytes) return;

    /* Fix up row->values pointers for L1 entry */
    for (int i = 0; i < n; i++) {
        cur->drain_rows[i].values = &cur->drain_values[i * nc];
        cur->drain_rows[i].next = NULL;
    }

    /* Transfer ownership to L1 cache */
    clearprism_cache_store_l1(vtab->cache, cur->cache_key,
                               cur->drain_rows, cur->drain_values,
                               n, nc, byte_size);

    /* Ownership transferred — NULL out without freeing */
    cur->drain_rows = NULL;
    cur->drain_values = NULL;
    cur->drain_n_rows = 0;
    cur->drain_n_cols = 0;
    sqlite3_free(cur->cache_key);
    cur->cache_key = NULL;
}

/* Get current source handle, or NULL */
static inline clearprism_source_handle *cursor_current_handle(clearprism_cursor *cur)
{
    if (cur->current_handle_idx >= 0 && cur->current_handle_idx < cur->n_handles)
        return &cur->handles[cur->current_handle_idx];
    return NULL;
}

/* Convenience: get stmt from current handle */
static inline sqlite3_stmt *cursor_current_stmt(clearprism_cursor *cur)
{
    clearprism_source_handle *h = cursor_current_handle(cur);
    return h ? h->stmt : NULL;
}

/* Convenience: get source index from current handle */
static inline int cursor_current_source_idx(clearprism_cursor *cur)
{
    clearprism_source_handle *h = cursor_current_handle(cur);
    return h ? h->source_idx : 0;
}

/* ========== xBestIndex ========== */

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
            int max_argv = 0;
            for (int j = 0; j < info->nConstraint; j++) {
                if (info->aConstraintUsage[j].argvIndex > max_argv)
                    max_argv = info->aConstraintUsage[j].argvIndex;
            }
            info->aConstraintUsage[i].argvIndex = max_argv + 1;
            info->aConstraintUsage[i].omit = 1;
            flags |= CLEARPRISM_PLAN_ROWID_LOOKUP;
            rowid_argv = max_argv + 1;

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

    /* ORDER BY pushdown: single-source = direct, multi-source = merge-sort */
    if (info->nOrderBy > 0 && !(flags & CLEARPRISM_PLAN_ROWID_LOOKUP)) {
        int can_order = 1;
        for (int i = 0; i < info->nOrderBy; i++) {
            int col = info->aOrderBy[i].iColumn;
            if (col < 0 || col >= vtab->nCol) { can_order = 0; break; }
        }
        if (can_order) {
            info->orderByConsumed = 1;
            flags |= CLEARPRISM_PLAN_HAS_ORDER;
            if (!(flags & CLEARPRISM_PLAN_SOURCE_CONSTRAINED)) {
                flags |= CLEARPRISM_PLAN_HAS_MERGE_ORDER;
            }
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

    /* Refined cost estimation using actual source count */
    int n_sources = vtab->registry ? vtab->registry->n_sources : 1;
    if (n_sources < 1) n_sources = 1;

    double cost;
    if (flags & CLEARPRISM_PLAN_ROWID_LOOKUP) {
        cost = 1.0;
        info->estimatedRows = 1;
    } else if (flags & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) {
        cost = 1000.0;
        if (flags & CLEARPRISM_PLAN_HAS_WHERE) cost *= 0.1;
        info->estimatedRows = 100;
    } else {
        cost = 1000.0 * n_sources;
        if (flags & CLEARPRISM_PLAN_HAS_WHERE) {
            int n_eq = 0, n_range = 0;
            for (int i = 0; i < info->nConstraint; i++) {
                if (!info->aConstraint[i].usable) continue;
                int col = info->aConstraint[i].iColumn;
                if (col < 0 || col >= vtab->nCol) continue;
                switch (info->aConstraint[i].op) {
                    case SQLITE_INDEX_CONSTRAINT_EQ: n_eq++; break;
                    case SQLITE_INDEX_CONSTRAINT_GT:
                    case SQLITE_INDEX_CONSTRAINT_GE:
                    case SQLITE_INDEX_CONSTRAINT_LT:
                    case SQLITE_INDEX_CONSTRAINT_LE: n_range++; break;
                    default: break;
                }
            }
            if (n_eq > 0) cost *= 0.01;
            else if (n_range > 0) cost *= 0.1;
            else cost *= 0.3;
        }
        info->estimatedRows = 1000 * n_sources;
    }
    if (flags & CLEARPRISM_PLAN_HAS_LIMIT) cost *= 0.5;
    info->estimatedCost = cost;

    (void)rowid_argv;
    info->idxNum = flags;
    info->idxStr = idx_str;
    info->needToFreeIdxStr = 1;

    return SQLITE_OK;
}

/* ========== xFilter ========== */

int clearprism_vtab_filter(sqlite3_vtab_cursor *pCur, int idxNum,
                            const char *idxStr, int argc, sqlite3_value **argv)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;
    clearprism_vtab *vtab = cur->vtab;

    /* Clean up previous iteration state */
    cursor_free_drain(cur);
    cursor_cleanup_handles(cur);
    cursor_free_alias_values(cur);
    sqlite3_free(cur->cached_sql); cur->cached_sql = NULL;
    sqlite3_free(cur->cached_fallback_sql); cur->cached_fallback_sql = NULL;
    clearprism_query_plan_clear(&cur->plan);
    clearprism_sources_free(cur->sources, cur->n_sources);
    cur->sources = NULL;
    cur->n_sources = 0;
    cur->current_handle_idx = -1;
    cur->lazy_prepare = 0;
    cur->row_counter = 0;
    cur->eof = 0;
    cur->serving_from_cache = 0;

    /* Free merge-sort heap */
    sqlite3_free(cur->heap);
    cur->heap = NULL;
    cur->heap_size = 0;

    /* Free saved argv */
    if (cur->saved_argv) {
        for (int i = 0; i < cur->saved_argc; i++)
            sqlite3_value_free(cur->saved_argv[i]);
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
        for (int i = 0; i < cur->total_in_values; i++)
            sqlite3_value_free(cur->in_values[i]);
        sqlite3_free(cur->in_values);
        cur->in_values = NULL;
    }
    sqlite3_free(cur->in_offsets); cur->in_offsets = NULL;
    sqlite3_free(cur->in_counts);  cur->in_counts = NULL;
    cur->n_in_constraints = 0;
    cur->total_in_values = 0;

    /* Decode plan (decode zeroes the struct, so set flags after) */
    int rc = clearprism_where_decode(idxStr, &cur->plan);
    cur->plan.flags = idxNum;
    if (rc != SQLITE_OK) {
        cur->eof = 1;
        return rc;
    }

    /* Extract source alias */
    if (idxNum & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) {
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].col_idx == vtab->nCol &&
                cur->plan.constraints[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc) {
                    const char *alias = (const char *)sqlite3_value_text(argv[ai]);
                    if (alias)
                        cur->plan.source_alias = clearprism_strdup(alias);
                }
                break;
            }
        }
    }

    /* Extract LIMIT */
    cur->limit_remaining = -1;
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

    /* Extract OFFSET */
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

    /* Expand IN constraints */
#if SQLITE_VERSION_NUMBER >= 3038000
    {
        int n_in = 0;
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].is_in) n_in++;
        }
        if (n_in > 0) {
            cur->n_in_constraints = n_in;
            cur->in_offsets = sqlite3_malloc(n_in * (int)sizeof(int));
            cur->in_counts = sqlite3_malloc(n_in * (int)sizeof(int));
            int in_idx = 0, total = 0;
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
                if (cur->in_counts)  cur->in_counts[in_idx] = count;
                cur->plan.constraints[i].in_count = count;
                cur->plan.constraints[i].in_offset = total;
                total += count;
                in_idx++;
            }
            cur->total_in_values = total;
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

    /* Save argv for binding */
    if (argc > 0 && argv) {
        cur->saved_argc = argc;
        cur->saved_argv = sqlite3_malloc(argc * (int)sizeof(sqlite3_value *));
        if (cur->saved_argv) {
            for (int i = 0; i < argc; i++)
                cur->saved_argv[i] = sqlite3_value_dup(argv[i]);
        }
    }

    /* Cache lookup */
    char cache_key[512];
    cursor_build_cache_key(cur, argc, argv, cache_key, sizeof(cache_key));

    if (vtab->cache) {
        clearprism_cache_cursor *cc = NULL;
        int cache_hit = clearprism_cache_lookup(vtab->cache, cache_key, &cc);
        if (cache_hit && cc) {
            cur->cache_cursor = cc;
            cur->serving_from_cache = 1;
            if (clearprism_cache_cursor_eof(cc))
                cur->eof = 1;
            return SQLITE_OK;
        }
    }

    cursor_free_buffer(cur);
    cur->cache_key = clearprism_strdup(cache_key);

    /* Rowid lookup fast-path */
    if (idxNum & CLEARPRISM_PLAN_ROWID_LOOKUP) {
        int64_t composite = 0;
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].col_idx == -1 &&
                cur->plan.constraints[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc)
                    composite = sqlite3_value_int64(argv[ai]);
                break;
            }
        }
        int64_t target_source_id = composite >> CLEARPRISM_ROWID_SHIFT;
        int64_t target_rowid = composite & CLEARPRISM_ROWID_MASK;

        char *snap_err = NULL;
        rc = clearprism_registry_snapshot(vtab->registry, vtab->target_table,
                                           &cur->sources, &cur->n_sources, &snap_err);
        sqlite3_free(snap_err);

        int found = -1;
        for (int i = 0; i < cur->n_sources; i++) {
            if (cur->sources[i].id == target_source_id) { found = i; break; }
        }
        if (found < 0) { cur->eof = 1; return SQLITE_OK; }

        cursor_precreate_alias_values(cur);

        /* Create a single handle for the rowid lookup */
        cur->handles = sqlite3_malloc(sizeof(clearprism_source_handle));
        if (!cur->handles) { cur->eof = 1; return SQLITE_OK; }
        memset(cur->handles, 0, sizeof(clearprism_source_handle));
        cur->n_handles = 1;
        cur->handles[0].source_idx = found;

        char *pool_err = NULL;
        sqlite3 *conn = clearprism_connpool_checkout(vtab->pool,
                            cur->sources[found].path,
                            cur->sources[found].alias, &pool_err);
        sqlite3_free(pool_err);
        if (!conn) { cur->eof = 1; return SQLITE_OK; }
        cur->handles[0].conn = conn;

        size_t sql_size = 256;
        for (int i = 0; i < vtab->nCol; i++)
            sql_size += strlen(vtab->cols[i].name) + 4;
        char *sql = sqlite3_malloc((int)sql_size);
        int p = snprintf(sql, sql_size, "SELECT rowid");
        for (int i = 0; i < vtab->nCol; i++)
            p += snprintf(sql + p, sql_size - p, ", \"%s\"", vtab->cols[i].name);
        p += snprintf(sql + p, sql_size - p, " FROM \"%s\" WHERE rowid = ?",
                       vtab->target_table);

        rc = sqlite3_prepare_v2(conn, sql, -1, &cur->handles[0].stmt, NULL);
        sqlite3_free(sql);
        if (rc != SQLITE_OK) {
            clearprism_connpool_checkin(vtab->pool, cur->sources[found].path);
            cur->handles[0].conn = NULL;
            cur->eof = 1;
            return SQLITE_OK;
        }
        sqlite3_bind_int64(cur->handles[0].stmt, 1, target_rowid);
        rc = sqlite3_step(cur->handles[0].stmt);
        if (rc == SQLITE_ROW) {
            cur->handles[0].has_row = 1;
            cur->current_handle_idx = 0;
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

    /* Filter to matching source if constrained */
    if (cur->plan.source_alias) {
        int found = 0;
        for (int i = 0; i < cur->n_sources; i++) {
            if (strcmp(cur->sources[i].alias, cur->plan.source_alias) == 0) {
                if (i != 0) {
                    clearprism_source tmp = cur->sources[0];
                    cur->sources[0] = cur->sources[i];
                    cur->sources[i] = tmp;
                }
                cur->n_sources = 1;
                found = 1;
                break;
            }
        }
        if (!found) cur->n_sources = 0;
    }

    if (cur->n_sources == 0 || cur->limit_remaining == 0) {
        cur->eof = 1;
        return SQLITE_OK;
    }

    /* Pre-create sqlite3_value for each source's alias (avoids per-row DB open) */
    cursor_precreate_alias_values(cur);

    /* Allocate handles — one per source */
    cur->n_handles = cur->n_sources;
    cur->handles = sqlite3_malloc(cur->n_handles * (int)sizeof(clearprism_source_handle));
    if (!cur->handles) {
        cur->eof = 1;
        return SQLITE_NOMEM;
    }
    memset(cur->handles, 0, cur->n_handles * (int)sizeof(clearprism_source_handle));
    for (int i = 0; i < cur->n_handles; i++)
        cur->handles[i].source_idx = i;

    /* Determine LIMIT pushdown: push LIMIT when ORDER BY present (each source's
     * top-N is a superset of its possible contribution to the global top-N) or
     * when single source (LIMIT is exact). Never push without ORDER BY on
     * multi-source because row ordering is arbitrary across sources. */
    int64_t pushdown_limit = -1;
    if (cur->limit_remaining > 0) {
        if (idxNum & CLEARPRISM_PLAN_HAS_ORDER) {
            pushdown_limit = cur->limit_remaining;
            if (cur->offset_remaining > 0)
                pushdown_limit += cur->offset_remaining;
        } else if (idxNum & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) {
            pushdown_limit = cur->limit_remaining;
            if (cur->offset_remaining > 0)
                pushdown_limit += cur->offset_remaining;
        }
    }

    /* Pre-generate SQL once for all sources */
    cur->cached_sql = clearprism_where_generate_sql(vtab->target_table,
                          vtab->cols, vtab->nCol, &cur->plan, pushdown_limit);
    if (!cur->cached_sql) {
        cur->eof = 1;
        return SQLITE_ERROR;
    }

    /* Pre-generate fallback SQL (without REGEXP/MATCH) if needed */
    {
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
            clearprism_query_plan fallback = cur->plan;
            int orig_n = cur->plan.n_constraints;
            clearprism_where_constraint *fb_cons = sqlite3_malloc(
                orig_n * (int)sizeof(*fb_cons));
            int fb_n = 0;
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
                    if (!skip) fb_cons[fb_n++] = cur->plan.constraints[i];
                }
                fallback.constraints = fb_cons;
                fallback.n_constraints = fb_n;
                cur->cached_fallback_sql = clearprism_where_generate_sql(
                    vtab->target_table, vtab->cols, vtab->nCol, &fallback,
                    pushdown_limit);
                sqlite3_free(fb_cons);
            }
        }
    }

    /* Ensure pool can hold all source connections simultaneously */
    if (vtab->pool && cur->n_handles > vtab->pool->max_open) {
        pthread_mutex_lock(&vtab->pool->lock);
        vtab->pool->max_open = cur->n_handles;
        pthread_mutex_unlock(&vtab->pool->lock);
    }

    /* Determine preparation mode:
     * Eager (parallel): ORDER BY, drain candidates — need all sources ready
     * Lazy (on-demand): Sequential full scans — prepare as cursor advances */
    int need_eager = (idxNum & CLEARPRISM_PLAN_HAS_MERGE_ORDER)
                   || (idxNum & CLEARPRISM_PLAN_HAS_WHERE)
                   || (idxNum & CLEARPRISM_PLAN_HAS_LIMIT)
                   || (idxNum & CLEARPRISM_PLAN_SOURCE_CONSTRAINED)
                   || (cur->n_handles <= 1);

    if (need_eager) {
        cur->lazy_prepare = 0;
        struct parallel_prepare_ctx ctx;
        ctx.cur = cur;
        ctx.next_handle = 0;

        int n_threads = cur->n_handles;
        if (n_threads > CLEARPRISM_MAX_PREPARE_THREADS)
            n_threads = CLEARPRISM_MAX_PREPARE_THREADS;
        if (cur->n_handles < CLEARPRISM_MIN_PARALLEL_SOURCES)
            n_threads = 1;
        if (n_threads < 1) n_threads = 1;

        if (n_threads == 1) {
            for (int i = 0; i < cur->n_handles; i++)
                handle_prepare_and_step(cur, &cur->handles[i]);
        } else {
            pthread_t *threads = sqlite3_malloc(n_threads * (int)sizeof(pthread_t));
            if (!threads) {
                for (int i = 0; i < cur->n_handles; i++)
                    handle_prepare_and_step(cur, &cur->handles[i]);
            } else {
                for (int i = 0; i < n_threads; i++)
                    pthread_create(&threads[i], NULL, parallel_prepare_worker, &ctx);
                for (int i = 0; i < n_threads; i++)
                    pthread_join(threads[i], NULL);
                sqlite3_free(threads);
            }
        }
    } else {
        /* Lazy mode: prepare only the first source, rest on demand */
        cur->lazy_prepare = 1;
        cur->prefetch_active = 0;
        if (cur->n_handles > 0)
            handle_prepare_and_step(cur, &cur->handles[0]);
        /* Start prefetching second source in background */
        if (cur->n_handles > 1) {
            cur->prefetch_next_idx = 1;
            if (pthread_create(&cur->prefetch_thread, NULL, prefetch_worker, cur) == 0)
                cur->prefetch_active = 1;
        }
    }

    /* Early buffer skip: predict uncacheable queries to avoid wasted
     * sqlite3_value_dup work.  Setting buffer_overflow = 1 here means
     * cursor_buffer_current_row() will short-circuit on the very first
     * check, saving up to l1_max_rows * nCol dup/free pairs. */
    if (!vtab->cache) {
        cur->buffer_overflow = 1;
    } else if (!(idxNum & CLEARPRISM_PLAN_HAS_WHERE) &&
               !(idxNum & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) &&
               cur->n_handles > 1) {
        /* Unconstrained multi-source scan — almost certainly exceeds L1 */
        cur->buffer_overflow = 1;
    }

    /* Parallel drain: materialize all rows from all sources into flat buffer.
     * This eliminates per-row vtab xNext/xColumn overhead for selective queries. */
    if (cur->n_handles > 1 && cursor_try_parallel_drain(cur, idxNum)) {
        /* Drain succeeded — cursor positioned at first row (drain_idx = 0) */
        if (cur->drain_n_rows == 0) {
            cur->eof = 1;
        }

        /* Skip OFFSET rows */
        while (cur->offset_remaining > 0 && cur->drain_idx < cur->drain_n_rows) {
            cur->offset_remaining--;
            cur->drain_idx++;
            cur->row_counter++;
        }
        if (cur->drain_idx >= cur->drain_n_rows) cur->eof = 1;

        return SQLITE_OK;
    }

    /* Position cursor on first handle with rows */
    if (idxNum & CLEARPRISM_PLAN_HAS_MERGE_ORDER) {
        /* Merge-sort: build heap from handles with rows */
        cur->heap = sqlite3_malloc(cur->n_handles * (int)sizeof(int));
        cur->heap_size = 0;
        if (cur->heap) {
            for (int i = 0; i < cur->n_handles; i++) {
                if (cur->handles[i].has_row && !cur->handles[i].errored)
                    cur->heap[cur->heap_size++] = i;
            }
            if (cur->heap_size > 0) {
                heap_build(cur);
                cur->current_handle_idx = cur->heap[0];
                cursor_buffer_current_row(cur);
            } else {
                cur->eof = 1;
            }
        } else {
            cur->eof = 1;
        }
    } else {
        /* Sequential: find first handle with rows */
        cur->current_handle_idx = -1;
        for (int i = 0; i < cur->n_handles; i++) {
            if (cur->handles[i].has_row && !cur->handles[i].errored) {
                cur->current_handle_idx = i;
                break;
            }
        }
        if (cur->current_handle_idx < 0) {
            cur->eof = 1;
        } else {
            cursor_buffer_current_row(cur);
        }
    }

    /* Skip OFFSET rows */
    while (cur->offset_remaining > 0 && !cur->eof) {
        cur->offset_remaining--;
        cur->row_counter++;

        if (idxNum & CLEARPRISM_PLAN_HAS_MERGE_ORDER) {
            /* Merge-sort advance */
            int top = cur->heap[0];
            clearprism_source_handle *h = &cur->handles[top];
            rc = sqlite3_step(h->stmt);
            if (rc == SQLITE_ROW) {
                heap_sift_down(cur, 0);
            } else {
                cur->heap[0] = cur->heap[--cur->heap_size];
                if (cur->heap_size > 0) heap_sift_down(cur, 0);
                sqlite3_finalize(h->stmt); h->stmt = NULL;
                if (h->conn) {
                    clearprism_connpool_checkin(vtab->pool,
                        cur->sources[h->source_idx].path);
                    h->conn = NULL;
                }
            }
            if (cur->heap_size == 0) { cur->eof = 1; break; }
            cur->current_handle_idx = cur->heap[0];
        } else {
            /* Sequential advance */
            clearprism_source_handle *h = cursor_current_handle(cur);
            if (h && h->stmt) {
                rc = sqlite3_step(h->stmt);
                if (rc == SQLITE_ROW) continue;
                rc = cursor_advance_source(cur);
                if (rc != SQLITE_OK) { cur->eof = 1; break; }
            } else {
                cur->eof = 1;
            }
        }
    }

    /* Buffer the first visible row if we skipped offset rows */
    if (!cur->eof && cur->offset_remaining == 0 && cur->row_counter > 0) {
        cursor_buffer_current_row(cur);
    }

    return SQLITE_OK;
}

/* ========== xNext ========== */

int clearprism_vtab_next(sqlite3_vtab_cursor *pCur)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;

    cur->row_counter++;

    /* Check LIMIT */
    if (cur->limit_remaining > 0) {
        cur->limit_remaining--;
        if (cur->limit_remaining == 0) {
            cur->eof = 1;
            if (cur->serving_from_drain)
                cursor_flush_drain_to_l1(cur);
            else
                cursor_flush_buffer_to_l1(cur);
            return SQLITE_OK;
        }
    }

    /* Drain serving path */
    if (cur->serving_from_drain) {
        cur->drain_idx++;
        if (cur->drain_idx >= cur->drain_n_rows) {
            cur->eof = 1;
            cursor_flush_drain_to_l1(cur);
        }
        return SQLITE_OK;
    }

    /* Cache serving path */
    if (cur->serving_from_cache && cur->cache_cursor) {
        clearprism_cache_cursor_next(cur->cache_cursor);
        if (clearprism_cache_cursor_eof(cur->cache_cursor))
            cur->eof = 1;
        return SQLITE_OK;
    }

    /* Merge-sort path */
    if (cur->plan.flags & CLEARPRISM_PLAN_HAS_MERGE_ORDER) {
        if (cur->heap_size == 0) {
            cur->eof = 1;
            cursor_flush_buffer_to_l1(cur);
            return SQLITE_OK;
        }
        int top = cur->heap[0];
        clearprism_source_handle *h = &cur->handles[top];
        int rc = sqlite3_step(h->stmt);
        if (rc == SQLITE_ROW) {
            heap_sift_down(cur, 0);
        } else {
            /* Source exhausted */
            cur->heap[0] = cur->heap[--cur->heap_size];
            if (cur->heap_size > 0) heap_sift_down(cur, 0);
            sqlite3_finalize(h->stmt); h->stmt = NULL;
            if (h->conn) {
                clearprism_connpool_checkin(cur->vtab->pool,
                    cur->sources[h->source_idx].path);
                h->conn = NULL;
            }
        }
        if (cur->heap_size == 0) {
            cur->eof = 1;
            cursor_flush_buffer_to_l1(cur);
        } else {
            cur->current_handle_idx = cur->heap[0];
            cursor_buffer_current_row(cur);
        }
        return SQLITE_OK;
    }

    /* Sequential live query path */
    sqlite3_stmt *stmt = cursor_current_stmt(cur);
    if (!stmt) {
        cur->eof = 1;
        cursor_flush_buffer_to_l1(cur);
        return SQLITE_OK;
    }

    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        cursor_buffer_current_row(cur);
        return SQLITE_OK;
    }

    /* Current source exhausted — advance to next */
    rc = cursor_advance_source(cur);
    if (rc != SQLITE_OK) {
        cur->eof = 1;
        cursor_flush_buffer_to_l1(cur);
    } else {
        cursor_buffer_current_row(cur);
    }
    return SQLITE_OK;
}

/* ========== xEof ========== */

int clearprism_vtab_eof(sqlite3_vtab_cursor *pCur)
{
    return ((clearprism_cursor *)pCur)->eof;
}

/* ========== xColumn ========== */

int clearprism_vtab_column(sqlite3_vtab_cursor *pCur, sqlite3_context *ctx,
                            int iCol)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;
    clearprism_vtab *vtab = cur->vtab;

    /* Drain serving path — zero-copy via SQLITE_STATIC */
    if (cur->serving_from_drain && cur->drain_values) {
        int idx = cur->drain_idx;
        if (idx >= 0 && idx < cur->drain_n_rows &&
            iCol >= 0 && iCol < cur->drain_n_cols) {
            sqlite3_value *val = cur->drain_values[idx * cur->drain_n_cols + iCol];
            if (!val) {
                sqlite3_result_null(ctx);
            } else {
                switch (sqlite3_value_type(val)) {
                case SQLITE_INTEGER:
                    sqlite3_result_int64(ctx, sqlite3_value_int64(val));
                    break;
                case SQLITE_FLOAT:
                    sqlite3_result_double(ctx, sqlite3_value_double(val));
                    break;
                case SQLITE_TEXT:
                    sqlite3_result_text(ctx,
                        (const char *)sqlite3_value_text(val),
                        sqlite3_value_bytes(val), SQLITE_STATIC);
                    break;
                case SQLITE_BLOB:
                    sqlite3_result_blob(ctx,
                        sqlite3_value_blob(val),
                        sqlite3_value_bytes(val), SQLITE_STATIC);
                    break;
                default:
                    sqlite3_result_null(ctx);
                    break;
                }
            }
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    /* Cache serving path — zero-copy via SQLITE_STATIC */
    if (cur->serving_from_cache && cur->cache_cursor) {
        sqlite3_value *val = clearprism_cache_cursor_value(cur->cache_cursor, iCol);
        if (!val) {
            sqlite3_result_null(ctx);
        } else {
            switch (sqlite3_value_type(val)) {
            case SQLITE_INTEGER:
                sqlite3_result_int64(ctx, sqlite3_value_int64(val));
                break;
            case SQLITE_FLOAT:
                sqlite3_result_double(ctx, sqlite3_value_double(val));
                break;
            case SQLITE_TEXT:
                sqlite3_result_text(ctx,
                    (const char *)sqlite3_value_text(val),
                    sqlite3_value_bytes(val), SQLITE_STATIC);
                break;
            case SQLITE_BLOB:
                sqlite3_result_blob(ctx,
                    sqlite3_value_blob(val),
                    sqlite3_value_bytes(val), SQLITE_STATIC);
                break;
            default:
                sqlite3_result_null(ctx);
                break;
            }
        }
        return SQLITE_OK;
    }

    /* Hidden _source_db column */
    if (iCol == vtab->nCol) {
        int si = cursor_current_source_idx(cur);
        const char *alias = (si < cur->n_sources) ? cur->sources[si].alias : NULL;
        if (alias)
            sqlite3_result_text(ctx, alias, -1, SQLITE_TRANSIENT);
        else
            sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    /* Real columns — offset by CLEARPRISM_COL_OFFSET because rowid is column 0 */
    sqlite3_stmt *stmt = cursor_current_stmt(cur);
    if (stmt && iCol >= 0 && iCol < vtab->nCol) {
        sqlite3_result_value(ctx, sqlite3_column_value(stmt,
                             iCol + CLEARPRISM_COL_OFFSET));
    } else {
        sqlite3_result_null(ctx);
    }

    return SQLITE_OK;
}

/* ========== xRowid ========== */

int clearprism_vtab_rowid(sqlite3_vtab_cursor *pCur, sqlite3_int64 *pRowid)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;

    if (cur->serving_from_drain && cur->drain_rows) {
        int idx = cur->drain_idx;
        if (idx >= 0 && idx < cur->drain_n_rows)
            *pRowid = cur->drain_rows[idx].composite_rowid;
        else
            *pRowid = cur->row_counter;
        return SQLITE_OK;
    }

    if (cur->serving_from_cache && cur->cache_cursor) {
        *pRowid = cur->cache_cursor->current_rowid;
        return SQLITE_OK;
    }

    sqlite3_stmt *stmt = cursor_current_stmt(cur);
    int si = cursor_current_source_idx(cur);
    if (stmt && si < cur->n_sources) {
        int64_t source_id = cur->sources[si].id;
        int64_t source_rowid = sqlite3_column_int64(stmt, 0);
        *pRowid = (source_id << CLEARPRISM_ROWID_SHIFT) |
                  (source_rowid & CLEARPRISM_ROWID_MASK);
    } else {
        *pRowid = cur->row_counter;
    }
    return SQLITE_OK;
}

/* ========== Internal helpers ========== */

/*
 * Prepare a single source handle: checkout connection, prepare SQL,
 * bind parameters, step to first row. Thread-safe (operates only on
 * the handle's own fields and the thread-safe connection pool).
 */
static void handle_prepare_and_step(clearprism_cursor *cur,
                                     clearprism_source_handle *h)
{
    clearprism_vtab *vtab = cur->vtab;
    int si = h->source_idx;
    if (si >= cur->n_sources) { h->errored = 1; return; }

    /* Checkout connection */
    char *pool_err = NULL;
    h->conn = clearprism_connpool_checkout(vtab->pool,
                  cur->sources[si].path, cur->sources[si].alias, &pool_err);
    sqlite3_free(pool_err);
    if (!h->conn) { h->errored = 1; return; }

    /* Prepare statement using pre-generated SQL */
    int used_fallback = 0;
    int rc = sqlite3_prepare_v2(h->conn, cur->cached_sql, -1, &h->stmt, NULL);

    /* REGEXP/MATCH fallback — use pre-generated fallback SQL */
    if (rc != SQLITE_OK && cur->cached_fallback_sql) {
        rc = sqlite3_prepare_v2(h->conn, cur->cached_fallback_sql,
                                 -1, &h->stmt, NULL);
        if (rc == SQLITE_OK) used_fallback = 1;
    }
    if (rc != SQLITE_OK) {
        clearprism_connpool_checkin(vtab->pool, cur->sources[si].path);
        h->conn = NULL;
        h->errored = 1;
        return;
    }

    /* Bind parameters */
    int bind_idx = 1;
    for (int i = 0; i < cur->plan.n_constraints; i++) {
        int col = cur->plan.constraints[i].col_idx;
        int op = cur->plan.constraints[i].op;
        if (col == vtab->nCol) continue;
        if (col < 0) continue;
        if (col >= vtab->nCol) continue;

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

        /* IN constraint */
        if (cur->plan.constraints[i].is_in && cur->in_values &&
            cur->plan.constraints[i].in_count > 0) {
            int off = cur->plan.constraints[i].in_offset;
            int cnt = cur->plan.constraints[i].in_count;
            for (int k = 0; k < cnt; k++) {
                if (off + k < cur->total_in_values && cur->in_values[off + k])
                    sqlite3_bind_value(h->stmt, bind_idx, cur->in_values[off + k]);
                bind_idx++;
            }
        } else {
            int ai = cur->plan.constraints[i].argv_idx - 1;
            if (ai >= 0 && ai < cur->saved_argc && cur->saved_argv &&
                cur->saved_argv[ai])
                sqlite3_bind_value(h->stmt, bind_idx, cur->saved_argv[ai]);
            bind_idx++;
        }
    }

    /* Step to first row */
    rc = sqlite3_step(h->stmt);
    h->has_row = (rc == SQLITE_ROW);
    if (rc != SQLITE_ROW && rc != SQLITE_DONE) h->errored = 1;
}

/*
 * Advance to the next source handle with rows (sequential mode).
 * Does NOT finalize the current handle — caller has already stepped past
 * the end or knows it's done with the current.
 */
static int cursor_advance_source(clearprism_cursor *cur)
{
    clearprism_vtab *vtab = cur->vtab;

    /* Join background prefetch before accessing next handle */
    if (cur->prefetch_active) {
        pthread_join(cur->prefetch_thread, NULL);
        cur->prefetch_active = 0;
    }

    /* Clean up current handle */
    clearprism_source_handle *h = cursor_current_handle(cur);
    if (h) {
        if (h->stmt) { sqlite3_finalize(h->stmt); h->stmt = NULL; }
        if (h->conn) {
            clearprism_connpool_checkin(vtab->pool,
                cur->sources[h->source_idx].path);
            h->conn = NULL;
        }
    }

    /* Find next handle with rows */
    while (++cur->current_handle_idx < cur->n_handles) {
        h = &cur->handles[cur->current_handle_idx];
        /* In lazy mode, prepare this handle on demand */
        if (cur->lazy_prepare && !h->stmt && !h->errored)
            handle_prepare_and_step(cur, h);
        if (h->has_row && !h->errored) {
            /* Start prefetching the handle after this one */
            int next = cur->current_handle_idx + 1;
            if (cur->lazy_prepare && next < cur->n_handles &&
                !cur->handles[next].stmt && !cur->handles[next].errored) {
                cur->prefetch_next_idx = next;
                if (pthread_create(&cur->prefetch_thread, NULL, prefetch_worker, cur) == 0)
                    cur->prefetch_active = 1;
            }
            return SQLITE_OK;
        }
        /* Clean up empty/errored handle */
        if (h->stmt) { sqlite3_finalize(h->stmt); h->stmt = NULL; }
        if (h->conn) {
            clearprism_connpool_checkin(vtab->pool,
                cur->sources[h->source_idx].path);
            h->conn = NULL;
        }
    }

    cur->eof = 1;
    return SQLITE_DONE;
}

/*
 * Free all source handles — finalize statements, checkin connections.
 */
static void cursor_cleanup_handles(clearprism_cursor *cur)
{
    if (!cur->handles) return;
    /* Join any active prefetch thread */
    if (cur->prefetch_active) {
        pthread_join(cur->prefetch_thread, NULL);
        cur->prefetch_active = 0;
    }
    clearprism_vtab *vtab = cur->vtab;
    for (int i = 0; i < cur->n_handles; i++) {
        clearprism_source_handle *h = &cur->handles[i];
        if (h->stmt) sqlite3_finalize(h->stmt);
        if (h->conn && h->source_idx < cur->n_sources)
            clearprism_connpool_checkin(vtab->pool,
                cur->sources[h->source_idx].path);
    }
    sqlite3_free(cur->handles);
    cur->handles = NULL;
    cur->n_handles = 0;
    cur->current_handle_idx = -1;
}

static void cursor_build_cache_key(clearprism_cursor *cur, int argc,
                                    sqlite3_value **argv,
                                    char *buf, int buf_size)
{
    clearprism_vtab *vtab = cur->vtab;
    int pos = 0;

    pos += snprintf(buf + pos, buf_size - pos, "%s:", vtab->target_table);

    if (cur->plan.source_alias)
        pos += snprintf(buf + pos, buf_size - pos, "src=%s:", cur->plan.source_alias);

    for (int i = 0; i < cur->plan.n_constraints; i++) {
        pos += snprintf(buf + pos, buf_size - pos, "c%d=%d:%d:",
                        i, cur->plan.constraints[i].col_idx,
                        cur->plan.constraints[i].op);
    }

    for (int i = 0; i < argc && i < cur->plan.n_constraints; i++) {
        int ai = cur->plan.constraints[i].argv_idx - 1;
        if (ai >= 0 && ai < argc && argv[ai]) {
            const char *txt = (const char *)sqlite3_value_text(argv[ai]);
            if (txt)
                pos += snprintf(buf + pos, buf_size - pos, "p%d=%s:", i, txt);
        }
    }
}

/*
 * Pre-create sqlite3_value objects for each source's alias string.
 * This avoids opening a :memory: DB per row in cursor_buffer_current_row().
 * Called once per xFilter after source snapshot is established.
 */
static void cursor_precreate_alias_values(clearprism_cursor *cur)
{
    if (cur->n_sources <= 0) return;

    cur->alias_values = sqlite3_malloc(cur->n_sources * (int)sizeof(sqlite3_value *));
    if (!cur->alias_values) return;
    memset(cur->alias_values, 0, cur->n_sources * (int)sizeof(sqlite3_value *));

    sqlite3 *tmp_db = NULL;
    sqlite3_open(":memory:", &tmp_db);
    if (!tmp_db) return;

    sqlite3_stmt *tmp_stmt = NULL;
    sqlite3_prepare_v2(tmp_db, "SELECT ?", -1, &tmp_stmt, NULL);
    if (!tmp_stmt) { sqlite3_close(tmp_db); return; }

    for (int i = 0; i < cur->n_sources; i++) {
        const char *alias = cur->sources[i].alias;
        if (alias) {
            sqlite3_bind_text(tmp_stmt, 1, alias, -1, SQLITE_TRANSIENT);
            sqlite3_step(tmp_stmt);
            cur->alias_values[i] = sqlite3_value_dup(sqlite3_column_value(tmp_stmt, 0));
            sqlite3_reset(tmp_stmt);
        }
    }

    sqlite3_finalize(tmp_stmt);
    sqlite3_close(tmp_db);
}

static void cursor_free_alias_values(clearprism_cursor *cur)
{
    if (!cur->alias_values) return;
    for (int i = 0; i < cur->n_sources; i++)
        sqlite3_value_free(cur->alias_values[i]);
    sqlite3_free(cur->alias_values);
    cur->alias_values = NULL;
}

/*
 * Buffer the current row into the L1 population buffer.
 * Also stores the composite rowid for Feature 4 (cache rowid stability).
 */
static void cursor_buffer_current_row(clearprism_cursor *cur)
{
    clearprism_vtab *vtab = cur->vtab;
    sqlite3_stmt *stmt = cursor_current_stmt(cur);
    int si = cursor_current_source_idx(cur);

    if (cur->serving_from_drain) return;
    if (cur->buffer_overflow || !cur->cache_key || !vtab->cache || !stmt) return;

    int n_cols = vtab->nCol + 1;

    /* Lazy allocation of flat buffer arrays on first row */
    if (!cur->buf_rows) {
        int capacity = (int)vtab->l1_max_rows;
        if (capacity <= 0) capacity = CLEARPRISM_DEFAULT_L1_MAX_ROWS;
        cur->buf_rows = sqlite3_malloc(capacity * (int)sizeof(clearprism_l1_row));
        cur->buf_values = sqlite3_malloc(capacity * n_cols * (int)sizeof(sqlite3_value *));
        if (!cur->buf_rows || !cur->buf_values) {
            sqlite3_free(cur->buf_rows);  cur->buf_rows = NULL;
            sqlite3_free(cur->buf_values); cur->buf_values = NULL;
            cur->buffer_overflow = 1;
            return;
        }
        cur->buf_capacity = capacity;
        cur->buf_n_cols = n_cols;
    }

    size_t row_overhead = sizeof(clearprism_l1_row) + n_cols * sizeof(sqlite3_value *);

    if (cur->buffer_n_rows >= cur->buf_capacity ||
        (int64_t)(cur->buffer_bytes + row_overhead) > vtab->l1_max_bytes) {
        cur->buffer_overflow = 1;
        cursor_free_buffer(cur);
        return;
    }

    int row_idx = cur->buffer_n_rows;
    clearprism_l1_row *row = &cur->buf_rows[row_idx];
    sqlite3_value **vals = &cur->buf_values[row_idx * n_cols];

    size_t row_bytes = row_overhead;

    /* Copy real columns — offset by COL_OFFSET */
    for (int i = 0; i < vtab->nCol; i++) {
        sqlite3_value *src = sqlite3_column_value(stmt, i + CLEARPRISM_COL_OFFSET);
        vals[i] = sqlite3_value_dup(src);
        row_bytes += clearprism_value_memsize(src);
    }

    /* Store composite rowid */
    if (si < cur->n_sources) {
        int64_t source_id = cur->sources[si].id;
        int64_t source_rowid = sqlite3_column_int64(stmt, 0);
        row->composite_rowid = (source_id << CLEARPRISM_ROWID_SHIFT) |
                               (source_rowid & CLEARPRISM_ROWID_MASK);
    } else {
        row->composite_rowid = cur->row_counter;
    }

    /* Copy _source_db as last value — use pre-created alias values */
    if (si < cur->n_sources && cur->alias_values && cur->alias_values[si]) {
        vals[vtab->nCol] = sqlite3_value_dup(cur->alias_values[si]);
        const char *alias = cur->sources[si].alias;
        if (alias) row_bytes += strlen(alias) + 1;
    } else {
        vals[vtab->nCol] = NULL;
    }

    row->n_values = n_cols;
    row->values = vals;
    row->next = NULL;

    cur->buffer_n_rows++;
    cur->buffer_bytes += row_bytes;
}

static void cursor_flush_buffer_to_l1(clearprism_cursor *cur)
{
    if (!cur->cache_key || !cur->vtab->cache || cur->buffer_overflow ||
        cur->buffer_n_rows == 0 || !cur->buf_rows) {
        cursor_free_buffer(cur);
        return;
    }

    int n_rows = cur->buffer_n_rows;
    int n_cols = cur->buf_n_cols;

    /* Fix up row->values pointers for the L1 entry */
    for (int i = 0; i < n_rows; i++) {
        cur->buf_rows[i].values = &cur->buf_values[i * n_cols];
        cur->buf_rows[i].next = NULL;
    }

    /* Hand off ownership of the pre-allocated flat arrays to L1 cache */
    clearprism_cache_store_l1(cur->vtab->cache, cur->cache_key,
                               cur->buf_rows, cur->buf_values,
                               n_rows, n_cols,
                               cur->buffer_bytes);

    /* Ownership transferred — NULL out without freeing */
    cur->buf_rows = NULL;
    cur->buf_values = NULL;
    cur->buf_capacity = 0;
    cur->buf_n_cols = 0;
    cur->buffer_n_rows = 0;
    cur->buffer_bytes = 0;
    sqlite3_free(cur->cache_key);
    cur->cache_key = NULL;
}

static void cursor_free_buffer(clearprism_cursor *cur)
{
    if (cur->buf_values) {
        int total = cur->buffer_n_rows * cur->buf_n_cols;
        for (int i = 0; i < total; i++)
            sqlite3_value_free(cur->buf_values[i]);
        sqlite3_free(cur->buf_values);
        cur->buf_values = NULL;
    }
    sqlite3_free(cur->buf_rows);
    cur->buf_rows = NULL;
    cur->buf_capacity = 0;
    cur->buf_n_cols = 0;
    cur->buffer_n_rows = 0;
    cur->buffer_bytes = 0;
    sqlite3_free(cur->cache_key);
    cur->cache_key = NULL;
    cur->buffer_overflow = 0;
}

/* ========== Merge-sort heap ========== */

static int merge_compare(clearprism_cursor *cur, int a_idx, int b_idx)
{
    clearprism_source_handle *a = &cur->handles[a_idx];
    clearprism_source_handle *b = &cur->handles[b_idx];

    for (int i = 0; i < cur->plan.n_order_cols; i++) {
        int col = cur->plan.order_cols[i].col_idx + CLEARPRISM_COL_OFFSET;
        sqlite3_value *va = sqlite3_column_value(a->stmt, col);
        sqlite3_value *vb = sqlite3_column_value(b->stmt, col);
        int cmp = clearprism_value_compare(va, vb);
        if (cur->plan.order_cols[i].desc) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    /* Stable: lower source index first */
    return a_idx - b_idx;
}

static void heap_sift_down(clearprism_cursor *cur, int i)
{
    while (1) {
        int smallest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;
        if (left < cur->heap_size &&
            merge_compare(cur, cur->heap[left], cur->heap[smallest]) < 0)
            smallest = left;
        if (right < cur->heap_size &&
            merge_compare(cur, cur->heap[right], cur->heap[smallest]) < 0)
            smallest = right;
        if (smallest == i) break;
        int tmp = cur->heap[i];
        cur->heap[i] = cur->heap[smallest];
        cur->heap[smallest] = tmp;
        i = smallest;
    }
}

static void heap_build(clearprism_cursor *cur)
{
    for (int i = cur->heap_size / 2 - 1; i >= 0; i--)
        heap_sift_down(cur, i);
}
