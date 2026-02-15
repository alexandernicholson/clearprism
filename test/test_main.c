/*
 * test_main.c — Test runner
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "clearprism.h"

/* Test function declarations */
extern int test_registry_run(void);
extern int test_connpool_run(void);
extern int test_cache_run(void);
extern int test_vtab_run(void);
extern int test_agg_run(void);
extern int test_scanner_run(void);
extern int test_admin_run(void);

/* Simple test framework */
static int total_tests = 0;
static int passed_tests = 0;
static int failed_tests = 0;

void test_report(const char *name, int passed)
{
    total_tests++;
    if (passed) {
        passed_tests++;
        printf("  PASS: %s\n", name);
    } else {
        failed_tests++;
        printf("  FAIL: %s\n", name);
    }
}

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;

    printf("=== Clearprism Test Suite ===\n\n");

    printf("[Registry Tests]\n");
    test_registry_run();
    printf("\n");

    printf("[Connection Pool Tests]\n");
    test_connpool_run();
    printf("\n");

    printf("[Cache Tests]\n");
    test_cache_run();
    printf("\n");

    printf("[Virtual Table Tests]\n");
    test_vtab_run();
    printf("\n");

    printf("[Aggregate Pushdown Tests]\n");
    test_agg_run();
    printf("\n");

    printf("[Scanner API Tests]\n");
    test_scanner_run();
    printf("\n");

    printf("[Admin Function Tests]\n");
    test_admin_run();
    printf("\n");

    printf("=== Results: %d/%d passed", passed_tests, total_tests);
    if (failed_tests > 0) {
        printf(", %d FAILED", failed_tests);
    }
    printf(" ===\n");

    return failed_tests > 0 ? 1 : 0;
}
