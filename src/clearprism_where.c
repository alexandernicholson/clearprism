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

        /* Only push down supported operations */
        switch (op) {
            case SQLITE_INDEX_CONSTRAINT_EQ:
            case SQLITE_INDEX_CONSTRAINT_GT:
            case SQLITE_INDEX_CONSTRAINT_GE:
            case SQLITE_INDEX_CONSTRAINT_LT:
            case SQLITE_INDEX_CONSTRAINT_LE:
            case SQLITE_INDEX_CONSTRAINT_LIKE:
                break;
            default:
                continue;  /* skip unsupported ops */
        }

        /* Check if this is the _source_db column (last column = nCol) */
        if (col == nCol && op == SQLITE_INDEX_CONSTRAINT_EQ) {
            flags |= CLEARPRISM_PLAN_SOURCE_CONSTRAINED;
        }

        flags |= CLEARPRISM_PLAN_HAS_WHERE;
        info->aConstraintUsage[i].argvIndex = argv_index++;
        info->aConstraintUsage[i].omit = (col == nCol) ? 1 : 0;
        /* Don't set omit for real columns — let SQLite double-check */
    }

    /* Build idxStr */
    /* Worst case: each constraint "999:999|" = ~8 chars, plus null */
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

    if (!idx_str || idx_str[0] == '\0') {
        return SQLITE_OK;  /* No constraints */
    }

    /* Count constraints (number of '|' + 1) */
    int count = 1;
    for (const char *p = idx_str; *p; p++) {
        if (*p == '|') count++;
    }

    plan->constraints = sqlite3_malloc(count * (int)sizeof(*plan->constraints));
    if (!plan->constraints) return SQLITE_NOMEM;

    /* Parse "colIdx:op" pairs */
    const char *p = idx_str;
    int i = 0;
    while (*p && i < count) {
        int col = 0, op = 0;
        /* Parse col */
        while (*p >= '0' && *p <= '9') {
            col = col * 10 + (*p - '0');
            p++;
        }
        if (*p == ':') p++;
        /* Parse op */
        while (*p >= '0' && *p <= '9') {
            op = op * 10 + (*p - '0');
            p++;
        }
        if (*p == '|') p++;

        plan->constraints[i].col_idx = col;
        plan->constraints[i].op = op;
        plan->constraints[i].argv_idx = i + 1;  /* 1-based */
        i++;
    }
    plan->n_constraints = i;
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
    /* Build column list */
    size_t col_buf_size = 256;
    for (int i = 0; i < nCol; i++) {
        col_buf_size += strlen(cols[i].name) + 4;
    }
    char *col_list = sqlite3_malloc((int)col_buf_size);
    if (!col_list) return NULL;
    col_list[0] = '\0';

    int pos = 0;
    for (int i = 0; i < nCol; i++) {
        if (i > 0) {
            pos += snprintf(col_list + pos, col_buf_size - pos, ", ");
        }
        pos += snprintf(col_list + pos, col_buf_size - pos, "\"%s\"", cols[i].name);
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
            switch (op) {
                case SQLITE_INDEX_CONSTRAINT_EQ:   op_str = "=";    break;
                case SQLITE_INDEX_CONSTRAINT_GT:   op_str = ">";    break;
                case SQLITE_INDEX_CONSTRAINT_GE:   op_str = ">=";   break;
                case SQLITE_INDEX_CONSTRAINT_LT:   op_str = "<";    break;
                case SQLITE_INDEX_CONSTRAINT_LE:   op_str = "<=";   break;
                case SQLITE_INDEX_CONSTRAINT_LIKE: op_str = "LIKE"; break;
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

            where_pos += snprintf(where + where_pos, where_size - where_pos,
                                   "\"%s\" %s ?", cols[col].name, op_str);
            param_count++;
        }
    }

    /* Assemble full SQL */
    char *sql;
    if (where && where[0] != '\0') {
        sql = clearprism_mprintf("SELECT %s FROM \"%s\" WHERE %s",
                                  col_list, table, where);
    } else {
        sql = clearprism_mprintf("SELECT %s FROM \"%s\"", col_list, table);
    }

    sqlite3_free(col_list);
    sqlite3_free(where);
    return sql;
}

void clearprism_query_plan_clear(clearprism_query_plan *plan)
{
    if (!plan) return;
    sqlite3_free(plan->constraints);
    sqlite3_free(plan->source_alias);
    memset(plan, 0, sizeof(*plan));
}
