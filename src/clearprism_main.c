/*
 * clearprism_main.c — Extension entry point, module registration
 */

#ifndef SQLITE_CORE
#define SQLITE_CORE 0
#endif

#if !SQLITE_CORE
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#else
#include <sqlite3.h>
#endif

#include "clearprism.h"

static sqlite3_module clearprism_module = {
#if SQLITE_VERSION_NUMBER >= 3038000
    3,                              /* iVersion — enables sqlite3_vtab_in */
#else
    0,                              /* iVersion */
#endif
    clearprism_vtab_create,         /* xCreate */
    clearprism_vtab_connect,        /* xConnect */
    clearprism_vtab_best_index,     /* xBestIndex */
    clearprism_vtab_disconnect,     /* xDisconnect */
    clearprism_vtab_destroy,        /* xDestroy */
    clearprism_vtab_open,           /* xOpen */
    clearprism_vtab_close,          /* xClose */
    clearprism_vtab_filter,         /* xFilter */
    clearprism_vtab_next,           /* xNext */
    clearprism_vtab_eof,            /* xEof */
    clearprism_vtab_column,         /* xColumn */
    clearprism_vtab_rowid,          /* xRowid */
    0,                              /* xUpdate (read-only) */
    0,                              /* xBegin */
    0,                              /* xSync */
    0,                              /* xCommit */
    0,                              /* xRollback */
    0,                              /* xFindFunction */
    0,                              /* xRename */
    0,                              /* xSavepoint */
    0,                              /* xRelease */
    0,                              /* xRollbackTo */
    0,                              /* xShadowName */
#if SQLITE_VERSION_NUMBER >= 3044000
    0,                              /* xIntegrity */
#endif
};

#if SQLITE_CORE
int clearprism_init(sqlite3 *db) {
    return sqlite3_create_module(db, "clearprism", &clearprism_module, NULL);
}
#endif

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_clearprism_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi)
{
#if !SQLITE_CORE
    SQLITE_EXTENSION_INIT2(pApi);
#endif
    (void)pzErrMsg;

    int rc = sqlite3_create_module(db, "clearprism", &clearprism_module, NULL);
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("clearprism: failed to register module: %s",
                                         sqlite3_errmsg(db));
        }
    }
    return rc;
}
