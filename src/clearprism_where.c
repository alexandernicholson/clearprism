/*
 * clearprism_where.c — WHERE constraint serialization & SQL generation for pushdown
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

/*
 * Encode xBestIndex constraints into an idxStr.
 * Format: pipe-delimited "colIdx:op" pairs, e.g. "0:2|3:64"
 * Op values are SQLITE_INDEX_CONSTRAINT_* constants.
 *
 * Also sets *out_flags with CLEARPRISM_PLAN_* bitmask.
 * Returns allocated idxStr (freed by SQLite via sqlite3_free).
 */
char *clearprism_where_encode(sqlite3_index_info *info, int nCol, int *out_flags)
{
    int flags = 0;
    int argv_index = 1;  /* 1-based as per SQLite convention */

    /* First pass: determine which constraints are usable and mark them */
    for (int i = 0; i < info->nConstraint; i++) {
        if (!info->aConstraint[i].usable) continue;

        int col = info->aConstraint[i].iColumn;
        int op = info->aConstraint[i].op;

        /* Skip rowid constraints (iColumn == -1) — handled in xBestIndex */
        if (col < 0) continue;

        /* Only push down supported operations */
        switch (op) {
            case SQLITE_INDEX_CONSTRAINT_EQ:
            case SQLITE_INDEX_CONSTRAINT_GT:
            case SQLITE_INDEX_CONSTRAINT_GE:
            case SQLITE_INDEX_CONSTRAINT_LT:
            case SQLITE_INDEX_CONSTRAINT_LE:
            case SQLITE_INDEX_CONSTRAINT_LIKE:
#ifdef SQLITE_INDEX_CONSTRAINT_NE
            case SQLITE_INDEX_CONSTRAINT_NE:
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_ISNULL
            case SQLITE_INDEX_CONSTRAINT_ISNULL:
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_ISNOTNULL
            case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_GLOB
            case SQLITE_INDEX_CONSTRAINT_GLOB:
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_REGEXP
            case SQLITE_INDEX_CONSTRAINT_REGEXP:
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_MATCH
            case SQLITE_INDEX_CONSTRAINT_MATCH:
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_LIMIT
            case SQLITE_INDEX_CONSTRAINT_LIMIT:
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_OFFSET
            case SQLITE_INDEX_CONSTRAINT_OFFSET:
#endif
                break;
            default:
                continue;  /* skip unsupported ops */
        }

        /* Check if this is the _source_db column (last column = nCol) */
        if (col == nCol && op == SQLITE_INDEX_CONSTRAINT_EQ) {
            flags |= CLEARPRISM_PLAN_SOURCE_CONSTRAINED;
        }

#ifdef SQLITE_INDEX_CONSTRAINT_LIMIT
        /* LIMIT constraint: consume it but don't add to WHERE clause */
        if (op == SQLITE_INDEX_CONSTRAINT_LIMIT) {
            flags |= CLEARPRISM_PLAN_HAS_LIMIT;
            info->aConstraintUsage[i].argvIndex = argv_index++;
            info->aConstraintUsage[i].omit = 1;
            continue;
        }
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_OFFSET
        /* OFFSET constraint: consume it but don't add to WHERE clause */
        if (op == SQLITE_INDEX_CONSTRAINT_OFFSET) {
            flags |= CLEARPRISM_PLAN_HAS_OFFSET;
            info->aConstraintUsage[i].argvIndex = argv_index++;
            info->aConstraintUsage[i].omit = 1;
            continue;
        }
#endif

        /* Detect IN constraints for EQ on non-_source_db columns */
#if SQLITE_VERSION_NUMBER >= 3038000
        if (op == SQLITE_INDEX_CONSTRAINT_EQ && col != nCol &&
            sqlite3_vtab_in(info, i, -1)) {
            sqlite3_vtab_in(info, i, 1);  /* request all-at-once IN processing */
        }
#endif

        flags |= CLEARPRISM_PLAN_HAS_WHERE;
        info->aConstraintUsage[i].argvIndex = argv_index++;
        info->aConstraintUsage[i].omit = (col == nCol) ? 1 : 0;
        /* Don't set omit for real columns — let SQLite double-check */
    }

    /* Build idxStr */
    /* Worst case: each constraint "999:999I|" = ~10 chars, plus null */
    int buf_size = (argv_index - 1) * 16 + 1;
    char *idx_str = sqlite3_malloc(buf_size);
    if (!idx_str) {
        *out_flags = 0;
        return NULL;
    }
    idx_str[0] = '\0';
    int pos = 0;

    for (int i = 0; i < info->nConstraint; i++) {
        if (info->aConstraintUsage[i].argvIndex <= 0) continue;

        int col = info->aConstraint[i].iColumn;
        int op = info->aConstraint[i].op;

        if (pos > 0) {
            idx_str[pos++] = '|';
        }
        pos += snprintf(idx_str + pos, buf_size - pos, "%d:%d", col, op);

        /* Mark IN constraints with 'I' suffix */
#if SQLITE_VERSION_NUMBER >= 3038000
        if (op == SQLITE_INDEX_CONSTRAINT_EQ && col != nCol &&
            sqlite3_vtab_in(info, i, -1)) {
            idx_str[pos++] = 'I';
        }
#endif
    }
    idx_str[pos] = '\0';

    /* Cost estimation */
    double base_cost = 1000000.0;
    if (flags & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) {
        /* Single source is much cheaper */
        base_cost = 10000.0;
    }
    if (flags & CLEARPRISM_PLAN_HAS_WHERE) {
        base_cost *= 0.1;
    }
    info->estimatedCost = base_cost;

    *out_flags = flags;
    return idx_str;
}

/*
 * Decode idxStr back into a query_plan structure.
 */
int clearprism_where_decode(const char *idx_str, clearprism_query_plan *plan)
{
    if (!plan) return SQLITE_ERROR;
    memset(plan, 0, sizeof(*plan));
    plan->limit_value = -1;
    plan->offset_value = 0;

    if (!idx_str || idx_str[0] == '\0') {
        return SQLITE_OK;  /* No constraints */
    }

    /* Find '#' separator for ORDER BY section */
    const char *hash = strchr(idx_str, '#');
    int constraint_len = hash ? (int)(hash - idx_str) : (int)strlen(idx_str);

    /* Count constraints (number of '|' + 1) in constraint section */
    int count = 0;
    if (constraint_len > 0) {
        count = 1;
        for (int j = 0; j < constraint_len; j++) {
            if (idx_str[j] == '|') count++;
        }
    }

    if (count > 0) {
        plan->constraints = sqlite3_malloc(count * (int)sizeof(*plan->constraints));
        if (!plan->constraints) return SQLITE_NOMEM;
        memset(plan->constraints, 0, count * (int)sizeof(*plan->constraints));

        /* Parse "colIdx:op" pairs — col can be negative (rowid = -1) */
        const char *p = idx_str;
        int i = 0;
        while (*p && *p != '#' && i < count) {
            int col = 0, neg = 0, op = 0;
            if (*p == '-') { neg = 1; p++; }
            while (*p >= '0' && *p <= '9') {
                col = col * 10 + (*p - '0');
                p++;
            }
            if (neg) col = -col;
            if (*p == ':') p++;
            while (*p >= '0' && *p <= '9') {
                op = op * 10 + (*p - '0');
                p++;
            }

            int is_in = 0;
            if (*p == 'I') { is_in = 1; p++; }
            if (*p == '|') p++;

            plan->constraints[i].col_idx = col;
            plan->constraints[i].op = op;
            plan->constraints[i].argv_idx = i + 1;
            plan->constraints[i].is_in = is_in;
            i++;
        }
        plan->n_constraints = i;
    }

    /* Parse ORDER BY section after '#' */
    if (hash && *(hash + 1)) {
        const char *p = hash + 1;
        /* Count order columns */
        int oc = 1;
        for (const char *q = p; *q; q++) {
            if (*q == '|') oc++;
        }
        plan->order_cols = sqlite3_malloc(oc * (int)sizeof(*plan->order_cols));
        if (plan->order_cols) {
            int i = 0;
            while (*p && i < oc) {
                int col = 0;
                while (*p >= '0' && *p <= '9') {
                    col = col * 10 + (*p - '0');
                    p++;
                }
                int desc = 0;
                if (*p == ':') p++;
                if (*p == 'D') { desc = 1; p++; }
                else if (*p == 'A') { p++; }
                if (*p == '|') p++;
                plan->order_cols[i].col_idx = col;
                plan->order_cols[i].desc = desc;
                i++;
            }
            plan->n_order_cols = i;
        }
    }

    return SQLITE_OK;
}

/*
 * Generate SQL SELECT statement with pushed-down WHERE constraints.
 * Returns allocated SQL string.
 */
char *clearprism_where_generate_sql(const char *table,
                                     clearprism_col_def *cols, int nCol,
                                     clearprism_query_plan *plan)
{
    /* Build column list — prepend rowid for composite rowid encoding */
    size_t col_buf_size = 256;
    for (int i = 0; i < nCol; i++) {
        col_buf_size += strlen(cols[i].name) + 4;
    }
    char *col_list = sqlite3_malloc((int)col_buf_size);
    if (!col_list) return NULL;
    col_list[0] = '\0';

    int pos = 0;
    pos += snprintf(col_list + pos, col_buf_size - pos, "rowid");
    for (int i = 0; i < nCol; i++) {
        pos += snprintf(col_list + pos, col_buf_size - pos, ", \"%s\"", cols[i].name);
    }

    /* Build WHERE clause from pushable constraints (exclude _source_db) */
    char *where = NULL;
    int where_pos = 0;
    int where_size = 0;
    int param_count = 0;

    if (plan && plan->n_constraints > 0) {
        where_size = 256;
        where = sqlite3_malloc(where_size);
        if (!where) {
            sqlite3_free(col_list);
            return NULL;
        }
        where[0] = '\0';

        for (int i = 0; i < plan->n_constraints; i++) {
            int col = plan->constraints[i].col_idx;
            int op = plan->constraints[i].op;

            /* Skip _source_db column — it's handled at the source selection level */
            if (col == nCol) continue;
            if (col < 0 || col >= nCol) continue;

            const char *op_str = NULL;
            int is_unary = 0;  /* 1 for IS NULL / IS NOT NULL (no ? param) */
            switch (op) {
                case SQLITE_INDEX_CONSTRAINT_EQ:   op_str = "=";    break;
                case SQLITE_INDEX_CONSTRAINT_GT:   op_str = ">";    break;
                case SQLITE_INDEX_CONSTRAINT_GE:   op_str = ">=";   break;
                case SQLITE_INDEX_CONSTRAINT_LT:   op_str = "<";    break;
                case SQLITE_INDEX_CONSTRAINT_LE:   op_str = "<=";   break;
                case SQLITE_INDEX_CONSTRAINT_LIKE: op_str = "LIKE"; break;
#ifdef SQLITE_INDEX_CONSTRAINT_NE
                case SQLITE_INDEX_CONSTRAINT_NE:   op_str = "!=";   break;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_GLOB
                case SQLITE_INDEX_CONSTRAINT_GLOB: op_str = "GLOB"; break;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_REGEXP
                case SQLITE_INDEX_CONSTRAINT_REGEXP: op_str = "REGEXP"; break;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_MATCH
                case SQLITE_INDEX_CONSTRAINT_MATCH: op_str = "MATCH"; break;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_ISNULL
                case SQLITE_INDEX_CONSTRAINT_ISNULL:
                    op_str = "IS NULL"; is_unary = 1; break;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_ISNOTNULL
                case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
                    op_str = "IS NOT NULL"; is_unary = 1; break;
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_LIMIT
                case SQLITE_INDEX_CONSTRAINT_LIMIT:
                    continue;  /* LIMIT is not a WHERE clause constraint */
#endif
#ifdef SQLITE_INDEX_CONSTRAINT_OFFSET
                case SQLITE_INDEX_CONSTRAINT_OFFSET:
                    continue;  /* OFFSET is not a WHERE clause constraint */
#endif
                default: continue;
            }

            if (param_count > 0) {
                where_pos += snprintf(where + where_pos, where_size - where_pos, " AND ");
            }

            int needed = (int)strlen(cols[col].name) + (int)strlen(op_str) + 32;
            if (where_pos + needed >= where_size) {
                where_size = where_size * 2 + needed;
                where = sqlite3_realloc(where, where_size);
                if (!where) {
                    sqlite3_free(col_list);
                    return NULL;
                }
            }

            if (is_unary) {
                where_pos += snprintf(where + where_pos, where_size - where_pos,
                                       "\"%s\" %s", cols[col].name, op_str);
            } else if (plan->constraints[i].is_in && plan->constraints[i].in_count > 0) {
                /* IN constraint: generate col IN (?,?,?) with correct count */
                int in_c = plan->constraints[i].in_count;
                int in_needed = (int)strlen(cols[col].name) + 16 + in_c * 2;
                if (where_pos + in_needed >= where_size) {
                    where_size = where_size * 2 + in_needed;
                    where = sqlite3_realloc(where, where_size);
                    if (!where) { sqlite3_free(col_list); return NULL; }
                }
                where_pos += snprintf(where + where_pos, where_size - where_pos,
                                       "\"%s\" IN (", cols[col].name);
                for (int k = 0; k < in_c; k++) {
                    if (k > 0) where_pos += snprintf(where + where_pos,
                                                       where_size - where_pos, ",");
                    where_pos += snprintf(where + where_pos,
                                           where_size - where_pos, "?");
                }
                where_pos += snprintf(where + where_pos, where_size - where_pos, ")");
            } else {
                where_pos += snprintf(where + where_pos, where_size - where_pos,
                                       "\"%s\" %s ?", cols[col].name, op_str);
            }
            param_count++;
        }
    }

    /* Build ORDER BY clause if plan has order columns */
    char *order_by = NULL;
    if (plan && plan->n_order_cols > 0 && cols) {
        int ob_size = 256;
        order_by = sqlite3_malloc(ob_size);
        if (order_by) {
            int ob_pos = 0;
            for (int i = 0; i < plan->n_order_cols; i++) {
                int ci = plan->order_cols[i].col_idx;
                if (ci < 0 || ci >= nCol) continue;
                if (ob_pos > 0) ob_pos += snprintf(order_by + ob_pos, ob_size - ob_pos, ", ");
                ob_pos += snprintf(order_by + ob_pos, ob_size - ob_pos, "\"%s\" %s",
                                    cols[ci].name, plan->order_cols[i].desc ? "DESC" : "ASC");
            }
        }
    }

    /* Assemble full SQL */
    char *sql;
    if (where && where[0] != '\0' && order_by && order_by[0] != '\0') {
        sql = clearprism_mprintf("SELECT %s FROM \"%s\" WHERE %s ORDER BY %s",
                                  col_list, table, where, order_by);
    } else if (where && where[0] != '\0') {
        sql = clearprism_mprintf("SELECT %s FROM \"%s\" WHERE %s",
                                  col_list, table, where);
    } else if (order_by && order_by[0] != '\0') {
        sql = clearprism_mprintf("SELECT %s FROM \"%s\" ORDER BY %s",
                                  col_list, table, order_by);
    } else {
        sql = clearprism_mprintf("SELECT %s FROM \"%s\"", col_list, table);
    }

    sqlite3_free(col_list);
    sqlite3_free(where);
    sqlite3_free(order_by);
    return sql;
}

void clearprism_query_plan_clear(clearprism_query_plan *plan)
{
    if (!plan) return;
    sqlite3_free(plan->constraints);
    sqlite3_free(plan->source_alias);
    sqlite3_free(plan->order_cols);
    memset(plan, 0, sizeof(*plan));
}
