/*
 * clearprism_util.c — String helpers, FNV-1a hash, error formatting
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

/* FNV-1a constants for 64-bit */
#define FNV1A_64_INIT  ((uint64_t)0xcbf29ce484222325ULL)
#define FNV1A_64_PRIME ((uint64_t)0x100000001b3ULL)

uint64_t clearprism_fnv1a(const void *data, size_t len)
{
    const unsigned char *p = (const unsigned char *)data;
    uint64_t hash = FNV1A_64_INIT;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint64_t)p[i];
        hash *= FNV1A_64_PRIME;
    }
    return hash;
}

uint64_t clearprism_fnv1a_str(const char *str)
{
    if (!str) return FNV1A_64_INIT;
    return clearprism_fnv1a(str, strlen(str));
}

char *clearprism_mprintf(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    /* Use sqlite3_vmprintf which returns a string that must be freed with sqlite3_free */
    char *result = sqlite3_vmprintf(fmt, ap);
    va_end(ap);
    return result;
}

void clearprism_set_errmsg(sqlite3_vtab *vtab, const char *fmt, ...)
{
    if (!vtab) return;
    sqlite3_free(vtab->zErrMsg);
    va_list ap;
    va_start(ap, fmt);
    vtab->zErrMsg = sqlite3_vmprintf(fmt, ap);
    va_end(ap);
}

char *clearprism_strdup(const char *s)
{
    if (!s) return NULL;
    size_t len = strlen(s);
    char *dup = sqlite3_malloc((int)(len + 1));
    if (dup) {
        memcpy(dup, s, len + 1);
    }
    return dup;
}

size_t clearprism_value_memsize(sqlite3_value *val)
{
    if (!val) return 0;
    int type = sqlite3_value_type(val);
    switch (type) {
        case SQLITE_INTEGER:
            return sizeof(int64_t);
        case SQLITE_FLOAT:
            return sizeof(double);
        case SQLITE_TEXT:
            return (size_t)sqlite3_value_bytes(val) + 1;
        case SQLITE_BLOB:
            return (size_t)sqlite3_value_bytes(val);
        case SQLITE_NULL:
        default:
            return 0;
    }
}

/*
 * Compare two sqlite3_value objects following SQLite's collation order:
 * NULL < INTEGER/FLOAT < TEXT < BLOB
 * Returns <0, 0, or >0 like strcmp.
 */
int clearprism_value_compare(sqlite3_value *a, sqlite3_value *b)
{
    int ta = a ? sqlite3_value_type(a) : SQLITE_NULL;
    int tb = b ? sqlite3_value_type(b) : SQLITE_NULL;

    /* Type ordering: NULL=0 < INTEGER/FLOAT=1 < TEXT=2 < BLOB=3
       SQLite constants: INTEGER=1, FLOAT=2, TEXT=3, BLOB=4, NULL=5 */
    static const int type_rank[6] = {
        0, /* 0: unused */
        1, /* 1: SQLITE_INTEGER */
        1, /* 2: SQLITE_FLOAT */
        2, /* 3: SQLITE_TEXT */
        3, /* 4: SQLITE_BLOB */
        0  /* 5: SQLITE_NULL */
    };

    int ra = (ta >= 1 && ta <= 5) ? type_rank[ta] : 0;
    int rb = (tb >= 1 && tb <= 5) ? type_rank[tb] : 0;

    if (ra != rb) return ra - rb;

    /* Same type group — compare values */
    if (ta == SQLITE_NULL) return 0;

    if (ta == SQLITE_INTEGER || ta == SQLITE_FLOAT ||
        tb == SQLITE_INTEGER || tb == SQLITE_FLOAT) {
        /* Numeric comparison: prefer double for mixed int/float */
        double da = sqlite3_value_double(a);
        double db = sqlite3_value_double(b);
        if (da < db) return -1;
        if (da > db) return 1;
        return 0;
    }

    if (ta == SQLITE_TEXT) {
        const char *sa = (const char *)sqlite3_value_text(a);
        const char *sb = (const char *)sqlite3_value_text(b);
        if (!sa && !sb) return 0;
        if (!sa) return -1;
        if (!sb) return 1;
        return strcmp(sa, sb);
    }

    if (ta == SQLITE_BLOB) {
        int na = sqlite3_value_bytes(a);
        int nb = sqlite3_value_bytes(b);
        const void *ba = sqlite3_value_blob(a);
        const void *bb = sqlite3_value_blob(b);
        int min_n = na < nb ? na : nb;
        int cmp = memcmp(ba, bb, min_n);
        if (cmp != 0) return cmp;
        return na - nb;
    }

    return 0;
}
