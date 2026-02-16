/*
 * bench_main.c — Clearprism read performance benchmark
 *
 * Usage: ./clearprism_bench [scenario_name]
 *   No args = run all scenarios (~50s)
 *   Scenario names: baseline, source_scale, cache, where, orderby, row_scale, concurrent, federation
 */

#define _POSIX_C_SOURCE 199309L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sqlite3.h>
#include "clearprism.h"

extern int clearprism_init(sqlite3 *db);

/* ========== Configuration ========== */

#define BENCH_WARMUP   3
#define BENCH_TMP_DIR  "/tmp/clearprism_bench"
#define BENCH_CSV_FILE "bench_results.csv"

/* ========== Fast LCG PRNG ========== */

static __thread uint32_t bench_rng_state;

static void bench_srand(uint32_t seed) { bench_rng_state = seed; }

static uint32_t bench_rand(void)
{
    bench_rng_state = bench_rng_state * 1103515245 + 12345;
    return (bench_rng_state >> 16) & 0x7FFF;
}

/* ========== Timing ========== */

static inline double bench_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec / 1e3;
}

typedef struct {
    double min_us, p50_us, p95_us, p99_us, max_us, mean_us;
    int64_t total_rows;
    int n_samples;
    double rows_per_sec;
} bench_stats;

typedef struct {
    double *samples;
    int n;
    int capacity;
} bench_timer;

static void timer_init(bench_timer *t, int capacity)
{
    t->samples = malloc(capacity * sizeof(double));
    t->n = 0;
    t->capacity = capacity;
}

static void timer_record(bench_timer *t, double us)
{
    if (t->n < t->capacity) t->samples[t->n++] = us;
}

static int dbl_cmp(const void *a, const void *b)
{
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static void timer_compute(bench_timer *t, int64_t total_rows, bench_stats *s)
{
    memset(s, 0, sizeof(*s));
    if (t->n == 0) return;
    qsort(t->samples, t->n, sizeof(double), dbl_cmp);
    s->n_samples = t->n;
    s->min_us = t->samples[0];
    s->max_us = t->samples[t->n - 1];
    s->p50_us = t->samples[t->n / 2];
    s->p95_us = t->samples[(int)(t->n * 0.95)];
    s->p99_us = t->samples[(int)(t->n * 0.99)];
    double sum = 0;
    for (int i = 0; i < t->n; i++) sum += t->samples[i];
    s->mean_us = sum / t->n;
    s->total_rows = total_rows;
    if (s->mean_us > 0)
        s->rows_per_sec = (double)total_rows / t->n / (s->mean_us / 1e6);
}

static void timer_free(bench_timer *t) { free(t->samples); }

/* ========== Output ========== */

static void print_header(void)
{
    printf("  %-20s | %5s | %10s | %10s | %10s | %10s | %10s | %10s\n",
           "Config", "Iters", "min", "p50", "p95", "p99", "max", "rows/s");
    printf("  %-20s-+-%5s-+-%10s-+-%10s-+-%10s-+-%10s-+-%10s-+-%10s\n",
           "--------------------", "-----", "----------", "----------",
           "----------", "----------", "----------", "----------");
}

static void fmt_us(char *buf, int sz, double us)
{
    if (us < 1000.0)
        snprintf(buf, sz, "%.0fus", us);
    else if (us < 1000000.0)
        snprintf(buf, sz, "%.2fms", us / 1000.0);
    else
        snprintf(buf, sz, "%.2fs", us / 1e6);
}

static void fmt_rate(char *buf, int sz, double rows_per_sec)
{
    if (rows_per_sec >= 1e6)
        snprintf(buf, sz, "%.2fM/s", rows_per_sec / 1e6);
    else if (rows_per_sec >= 1e3)
        snprintf(buf, sz, "%.1fK/s", rows_per_sec / 1e3);
    else
        snprintf(buf, sz, "%.0f/s", rows_per_sec);
}

static void print_row(const char *config, bench_stats *s)
{
    char mn[16], p50[16], p95[16], p99[16], mx[16], rate[16];
    fmt_us(mn, sizeof(mn), s->min_us);
    fmt_us(p50, sizeof(p50), s->p50_us);
    fmt_us(p95, sizeof(p95), s->p95_us);
    fmt_us(p99, sizeof(p99), s->p99_us);
    fmt_us(mx, sizeof(mx), s->max_us);
    fmt_rate(rate, sizeof(rate), s->rows_per_sec);
    printf("  %-20s | %5d | %10s | %10s | %10s | %10s | %10s | %10s\n",
           config, s->n_samples, mn, p50, p95, p99, mx, rate);
}

static void csv_append(const char *scenario, const char *config, bench_stats *s)
{
    FILE *f = fopen(BENCH_CSV_FILE, "a");
    if (!f) return;
    /* Write header if empty */
    fseek(f, 0, SEEK_END);
    if (ftell(f) == 0) {
        fprintf(f, "timestamp,scenario,config,iters,total_rows,"
                "min_us,p50_us,p95_us,p99_us,max_us,mean_us,rows_per_sec\n");
    }
    time_t now = time(NULL);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S", localtime(&now));
    fprintf(f, "%s,%s,%s,%d,%lld,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.0f\n",
            ts, scenario, config, s->n_samples, (long long)s->total_rows,
            s->min_us, s->p50_us, s->p95_us, s->p99_us, s->max_us,
            s->mean_us, s->rows_per_sec);
    fclose(f);
}

/* ========== Data Generation ========== */

static double bench_create_source_db(const char *path, int n_rows, uint32_t seed)
{
    double t0 = bench_now_us();
    sqlite3 *db;
    sqlite3_open(path, &db);
    sqlite3_exec(db, "PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF;", NULL, NULL, NULL);
    sqlite3_exec(db,
        "CREATE TABLE items ("
        "  id INTEGER PRIMARY KEY,"
        "  category TEXT NOT NULL,"
        "  name TEXT NOT NULL,"
        "  value REAL NOT NULL,"
        "  description TEXT,"
        "  created_at TEXT NOT NULL"
        ");"
        "CREATE INDEX idx_items_category ON items(category);"
        "CREATE INDEX idx_items_value ON items(value);",
        NULL, NULL, NULL);

    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db,
        "INSERT INTO items VALUES (?,?,?,?,?,?)", -1, &ins, NULL);

    bench_srand(seed);
    for (int i = 0; i < n_rows; i++) {
        int cat_id = bench_rand() % 100;
        double val = (double)(bench_rand() % 10000) + (double)(bench_rand() % 100) / 100.0;
        char cat[16], name[32], desc[256], ts[32];
        snprintf(cat, sizeof(cat), "cat_%03d", cat_id);
        snprintf(name, sizeof(name), "item_%06d", i);
        snprintf(desc, sizeof(desc),
                 "Description for item %d in category %s with value %.2f and some extra text to pad it",
                 i, cat, val);
        int day = i % 365;
        snprintf(ts, sizeof(ts), "2025-%02d-%02dT12:00:00Z",
                 (day / 30) % 12 + 1, (day % 30) + 1);

        sqlite3_bind_int(ins, 1, i + 1);
        sqlite3_bind_text(ins, 2, cat, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 3, name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(ins, 4, val);
        sqlite3_bind_text(ins, 5, desc, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 6, ts, -1, SQLITE_TRANSIENT);
        sqlite3_step(ins);
        sqlite3_reset(ins);
    }
    sqlite3_finalize(ins);
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);
    return (bench_now_us() - t0) / 1e6;
}

static void bench_create_registry(const char *reg_path,
                                   const char **src_paths,
                                   const char **aliases, int n)
{
    sqlite3 *db;
    sqlite3_open(reg_path, &db);
    sqlite3_exec(db,
        "CREATE TABLE clearprism_sources ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  path TEXT NOT NULL UNIQUE,"
        "  alias TEXT NOT NULL UNIQUE,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  priority INTEGER NOT NULL DEFAULT 0,"
        "  added_at TEXT NOT NULL DEFAULT (datetime('now')),"
        "  notes TEXT"
        ");"
        "CREATE TABLE clearprism_table_overrides ("
        "  source_id INTEGER NOT NULL REFERENCES clearprism_sources(id),"
        "  table_name TEXT NOT NULL,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  PRIMARY KEY (source_id, table_name)"
        ");",
        NULL, NULL, NULL);

    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db,
        "INSERT INTO clearprism_sources (path, alias, priority) VALUES (?,?,?)",
        -1, &ins, NULL);
    for (int i = 0; i < n; i++) {
        sqlite3_bind_text(ins, 1, src_paths[i], -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 2, aliases[i], -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(ins, 3, i);
        sqlite3_step(ins);
        sqlite3_reset(ins);
    }
    sqlite3_finalize(ins);
    sqlite3_close(db);
}

/* ========== Cleanup ========== */

static void bench_cleanup(void)
{
    /* Remove all files in bench tmp dir */
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", BENCH_TMP_DIR);
    (void)system(cmd);
}

static void bench_setup_dir(void)
{
    bench_cleanup();
    mkdir(BENCH_TMP_DIR, 0755);
}

/* ========== Query Runner ========== */

static void bench_run_query(sqlite3 *db, const char *sql,
                            int warmup, int iters,
                            bench_timer *timer, int64_t *out_rows)
{
    int64_t total_rows = 0;

    /* Warmup */
    for (int w = 0; w < warmup; w++) {
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            if (out_rows) *out_rows = 0;
            return;
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int nc = sqlite3_column_count(stmt);
            for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
        }
        sqlite3_finalize(stmt);
    }

    /* Measured iterations */
    for (int i = 0; i < iters; i++) {
        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);

        double t0 = bench_now_us();
        int64_t rows = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int nc = sqlite3_column_count(stmt);
            for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
            rows++;
        }
        double elapsed = bench_now_us() - t0;

        sqlite3_finalize(stmt);
        timer_record(timer, elapsed);
        total_rows += rows;
    }

    if (out_rows) *out_rows = total_rows;
}

/* Helper: create vtab and return db handle */
static sqlite3 *bench_open_vtab(const char *reg_path)
{
    sqlite3 *db;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);
    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE bench_items USING clearprism("
        "  registry_db='%s', table='items')", reg_path);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    return db;
}

/* ========== Scenario 1: Baseline Overhead ========== */

static void bench_scenario_baseline(void)
{
    printf("\n--- Baseline Overhead (1 source, 10K rows) ---\n");
    bench_setup_dir();

    char src_path[256], reg_path[256];
    snprintf(src_path, sizeof(src_path), "%s/src_0.db", BENCH_TMP_DIR);
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

    double gen_time = bench_create_source_db(src_path, 10000, 42);
    printf("  Data generated in %.2fs\n", gen_time);

    const char *paths[] = {src_path};
    const char *aliases[] = {"src_0"};
    bench_create_registry(reg_path, paths, aliases, 1);

    print_header();

    /* Direct SQLite */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        sqlite3 *db;
        sqlite3_open_v2(src_path, &db, SQLITE_OPEN_READONLY, NULL);
        bench_run_query(db, "SELECT * FROM items", BENCH_WARMUP, 100, &t, &rows);
        sqlite3_close(db);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("direct_sqlite", &s);
        csv_append("baseline", "direct_sqlite", &s);
        timer_free(&t);
    }

    /* Clearprism vtab */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        sqlite3 *db = bench_open_vtab(reg_path);
        bench_run_query(db, "SELECT * FROM bench_items", BENCH_WARMUP, 100, &t, &rows);
        sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
        sqlite3_close(db);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("clearprism_vtab", &s);
        csv_append("baseline", "clearprism_vtab", &s);
        timer_free(&t);
    }

    /* Direct SQLite filtered (indexed) */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        sqlite3 *db;
        sqlite3_open_v2(src_path, &db, SQLITE_OPEN_READONLY, NULL);
        bench_run_query(db, "SELECT * FROM items WHERE category = 'cat_042'",
                        BENCH_WARMUP, 100, &t, &rows);
        sqlite3_close(db);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("direct_filtered", &s);
        csv_append("baseline", "direct_filtered", &s);
        timer_free(&t);
    }

    /* Clearprism vtab filtered (cached after warmup) */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        sqlite3 *db = bench_open_vtab(reg_path);
        bench_run_query(db,
            "SELECT * FROM bench_items WHERE category = 'cat_042'",
            BENCH_WARMUP, 100, &t, &rows);
        sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
        sqlite3_close(db);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("vtab_cached_filt", &s);
        csv_append("baseline", "vtab_cached_filt", &s);
        timer_free(&t);
    }

    /* Multi-source comparison: 10 sources queried directly vs cached vtab */
    printf("\n--- Multi-Source Comparison (10 sources, 1K rows each) ---\n");
    {
        int n_src = 10;
        char msrc_paths[10][256], msrc_aliases[10][32];
        const char *mpath_ptrs[10], *malias_ptrs[10];
        char ms_reg_path[256];
        snprintf(ms_reg_path, sizeof(ms_reg_path), "%s/ms_registry.db", BENCH_TMP_DIR);

        for (int i = 0; i < n_src; i++) {
            snprintf(msrc_paths[i], sizeof(msrc_paths[i]), "%s/ms_src_%d.db", BENCH_TMP_DIR, i);
            snprintf(msrc_aliases[i], sizeof(msrc_aliases[i]), "ms_%02d", i);
            mpath_ptrs[i] = msrc_paths[i];
            malias_ptrs[i] = msrc_aliases[i];
            bench_create_source_db(msrc_paths[i], 1000, 100 + i);
        }
        bench_create_registry(ms_reg_path, mpath_ptrs, malias_ptrs, n_src);

        print_header();

        /* Simulate manual federation: query each source DB sequentially */
        {
            bench_timer t; timer_init(&t, 200);
            int64_t total_rows = 0;
            /* Warmup */
            for (int w = 0; w < BENCH_WARMUP; w++) {
                for (int s = 0; s < n_src; s++) {
                    sqlite3 *db;
                    sqlite3_open_v2(msrc_paths[s], &db, SQLITE_OPEN_READONLY, NULL);
                    sqlite3_stmt *stmt;
                    sqlite3_prepare_v2(db, "SELECT * FROM items WHERE category = 'cat_042'",
                                       -1, &stmt, NULL);
                    while (sqlite3_step(stmt) == SQLITE_ROW) {
                        int nc = sqlite3_column_count(stmt);
                        for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                    }
                    sqlite3_finalize(stmt);
                    sqlite3_close(db);
                }
            }
            /* Measured */
            for (int i = 0; i < 100; i++) {
                double t0 = bench_now_us();
                int64_t rows = 0;
                for (int s = 0; s < n_src; s++) {
                    sqlite3 *db;
                    sqlite3_open_v2(msrc_paths[s], &db, SQLITE_OPEN_READONLY, NULL);
                    sqlite3_stmt *stmt;
                    sqlite3_prepare_v2(db, "SELECT * FROM items WHERE category = 'cat_042'",
                                       -1, &stmt, NULL);
                    while (sqlite3_step(stmt) == SQLITE_ROW) {
                        int nc = sqlite3_column_count(stmt);
                        for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                        rows++;
                    }
                    sqlite3_finalize(stmt);
                    sqlite3_close(db);
                }
                double elapsed = bench_now_us() - t0;
                timer_record(&t, elapsed);
                total_rows += rows;
            }
            bench_stats s; timer_compute(&t, total_rows, &s);
            print_row("direct_10src_filt", &s);
            csv_append("baseline", "direct_10src_filt", &s);
            timer_free(&t);
        }

        /* Clearprism cached: same query, one vtab call */
        {
            bench_timer t; timer_init(&t, 200);
            int64_t rows = 0;
            sqlite3 *db;
            sqlite3_open(":memory:", &db);
            clearprism_init(db);
            char *sql = sqlite3_mprintf(
                "CREATE VIRTUAL TABLE ms_items USING clearprism("
                "  registry_db='%s', table='items')", ms_reg_path);
            sqlite3_exec(db, sql, NULL, NULL, NULL);
            sqlite3_free(sql);
            bench_run_query(db,
                "SELECT * FROM ms_items WHERE category = 'cat_042'",
                BENCH_WARMUP, 100, &t, &rows);
            sqlite3_exec(db, "DROP TABLE ms_items", NULL, NULL, NULL);
            sqlite3_close(db);
            bench_stats s; timer_compute(&t, rows, &s);
            print_row("vtab_10src_cached", &s);
            csv_append("baseline", "vtab_10src_cached", &s);
            timer_free(&t);
        }
    }

    bench_cleanup();
}

/* ========== Scenario 2: Source Scaling ========== */

static void bench_scenario_source_scale(void)
{
    printf("\n--- Source Scaling (10K rows total) ---\n");

    int configs[] = {1, 5, 10, 25, 50};
    int iters[]   = {50, 30, 20, 10, 5};
    int n_configs = sizeof(configs) / sizeof(configs[0]);

    print_header();

    for (int ci = 0; ci < n_configs; ci++) {
        bench_setup_dir();
        int n_src = configs[ci];
        int rows_per = 10000 / n_src;

        char reg_path[256];
        snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

        const char **paths = malloc(n_src * sizeof(char *));
        const char **aliases = malloc(n_src * sizeof(char *));
        char **path_bufs = malloc(n_src * sizeof(char *));
        char **alias_bufs = malloc(n_src * sizeof(char *));

        for (int i = 0; i < n_src; i++) {
            path_bufs[i] = malloc(256);
            alias_bufs[i] = malloc(32);
            snprintf(path_bufs[i], 256, "%s/src_%d.db", BENCH_TMP_DIR, i);
            snprintf(alias_bufs[i], 32, "src_%02d", i);
            paths[i] = path_bufs[i];
            aliases[i] = alias_bufs[i];
            bench_create_source_db(path_bufs[i], rows_per, 42 + i);
        }
        bench_create_registry(reg_path, paths, aliases, n_src);

        bench_timer t; timer_init(&t, iters[ci] + 10);
        int64_t rows = 0;
        sqlite3 *db = bench_open_vtab(reg_path);
        bench_run_query(db, "SELECT * FROM bench_items", BENCH_WARMUP, iters[ci], &t, &rows);
        sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
        sqlite3_close(db);

        bench_stats s; timer_compute(&t, rows, &s);
        char label[32];
        snprintf(label, sizeof(label), "%d sources", n_src);
        print_row(label, &s);
        csv_append("source_scale", label, &s);
        timer_free(&t);

        for (int i = 0; i < n_src; i++) { free(path_bufs[i]); free(alias_bufs[i]); }
        free(paths); free(aliases); free(path_bufs); free(alias_bufs);
        bench_cleanup();
    }
}

/* ========== Scenario 3: Cache Effectiveness ========== */

static void bench_scenario_cache(void)
{
    printf("\n--- Cache Effectiveness (10 sources, 1K rows each) ---\n");
    bench_setup_dir();

    int n_src = 10;
    char reg_path[256];
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

    const char *paths[10], *aliases[10];
    char path_bufs[10][256], alias_bufs[10][32];
    for (int i = 0; i < n_src; i++) {
        snprintf(path_bufs[i], sizeof(path_bufs[i]), "%s/src_%d.db", BENCH_TMP_DIR, i);
        snprintf(alias_bufs[i], sizeof(alias_bufs[i]), "src_%02d", i);
        paths[i] = path_bufs[i];
        aliases[i] = alias_bufs[i];
        bench_create_source_db(path_bufs[i], 1000, 42 + i);
    }
    bench_create_registry(reg_path, paths, aliases, n_src);

    print_header();

    /* Cold: each iteration uses a fresh vtab (no cache) */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t total_rows = 0;
        for (int i = 0; i < 50; i++) {
            sqlite3 *db = bench_open_vtab(reg_path);
            sqlite3_stmt *stmt;
            sqlite3_prepare_v2(db, "SELECT * FROM bench_items WHERE category = 'cat_042'",
                               -1, &stmt, NULL);
            double t0 = bench_now_us();
            int64_t rows = 0;
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                int nc = sqlite3_column_count(stmt);
                for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                rows++;
            }
            double elapsed = bench_now_us() - t0;
            sqlite3_finalize(stmt);
            timer_record(&t, elapsed);
            total_rows += rows;
            sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
            sqlite3_close(db);
        }
        bench_stats s; timer_compute(&t, total_rows, &s);
        print_row("cold_cache", &s);
        csv_append("cache", "cold_cache", &s);
        timer_free(&t);
    }

    /* Warm: same vtab, same query repeated (L1 hit after first) */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        sqlite3 *db = bench_open_vtab(reg_path);
        bench_run_query(db, "SELECT * FROM bench_items WHERE category = 'cat_042'",
                        1, 100, &t, &rows);
        sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
        sqlite3_close(db);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("warm_cache", &s);
        csv_append("cache", "warm_cache", &s);
        timer_free(&t);
    }

    /* Diverse: 20 different queries cycling, small L1 */
    {
        bench_timer t; timer_init(&t, 500);
        int64_t total_rows = 0;
        sqlite3 *db;
        sqlite3_open(":memory:", &db);
        clearprism_init(db);
        char *create_sql = sqlite3_mprintf(
            "CREATE VIRTUAL TABLE bench_items USING clearprism("
            "  registry_db='%s', table='items', l1_max_rows=500)", reg_path);
        sqlite3_exec(db, create_sql, NULL, NULL, NULL);
        sqlite3_free(create_sql);

        for (int round = 0; round < 5; round++) {
            for (int q = 0; q < 20; q++) {
                char sql[128];
                snprintf(sql, sizeof(sql),
                         "SELECT * FROM bench_items WHERE category = 'cat_%03d'", q * 5);
                sqlite3_stmt *stmt;
                sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
                double t0 = bench_now_us();
                int64_t rows = 0;
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    int nc = sqlite3_column_count(stmt);
                    for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                    rows++;
                }
                double elapsed = bench_now_us() - t0;
                sqlite3_finalize(stmt);
                timer_record(&t, elapsed);
                total_rows += rows;
            }
        }
        sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
        sqlite3_close(db);
        bench_stats s; timer_compute(&t, total_rows, &s);
        print_row("diverse_queries", &s);
        csv_append("cache", "diverse_queries", &s);
        timer_free(&t);
    }

    bench_cleanup();
}

/* ========== Scenario 4: WHERE Selectivity ========== */

static void bench_scenario_where(void)
{
    printf("\n--- WHERE Selectivity (10 sources, 1K rows each) ---\n");
    bench_setup_dir();

    int n_src = 10;
    char reg_path[256];
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

    const char *paths[10], *aliases[10];
    char path_bufs[10][256], alias_bufs[10][32];
    for (int i = 0; i < n_src; i++) {
        snprintf(path_bufs[i], sizeof(path_bufs[i]), "%s/src_%d.db", BENCH_TMP_DIR, i);
        snprintf(alias_bufs[i], sizeof(alias_bufs[i]), "src_%02d", i);
        paths[i] = path_bufs[i];
        aliases[i] = alias_bufs[i];
        bench_create_source_db(path_bufs[i], 1000, 42 + i);
    }
    bench_create_registry(reg_path, paths, aliases, n_src);

    sqlite3 *db = bench_open_vtab(reg_path);
    print_header();

    struct { const char *label; const char *sql; } queries[] = {
        {"full_scan",     "SELECT * FROM bench_items"},
        {"eq_name",       "SELECT * FROM bench_items WHERE name = 'item_000500'"},
        {"eq_category",   "SELECT * FROM bench_items WHERE category = 'cat_042'"},
        {"range_value",   "SELECT * FROM bench_items WHERE value BETWEEN 1000 AND 2000"},
        {"like_prefix",   "SELECT * FROM bench_items WHERE name LIKE 'item_00%'"},
        {"in_5_cats",     "SELECT * FROM bench_items WHERE category IN ('cat_001','cat_002','cat_003','cat_004','cat_005')"},
    };
    int n_queries = sizeof(queries) / sizeof(queries[0]);

    for (int q = 0; q < n_queries; q++) {
        bench_timer t; timer_init(&t, 100);
        int64_t rows = 0;
        bench_run_query(db, queries[q].sql, BENCH_WARMUP, 50, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row(queries[q].label, &s);
        csv_append("where", queries[q].label, &s);
        timer_free(&t);
    }

    sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
    sqlite3_close(db);
    bench_cleanup();
}

/* ========== Scenario 5: ORDER BY / Merge-Sort ========== */

static void bench_scenario_orderby(void)
{
    printf("\n--- ORDER BY / Merge-Sort (20 sources, 500 rows each) ---\n");
    bench_setup_dir();

    int n_src = 20;
    char reg_path[256];
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

    const char **paths = malloc(n_src * sizeof(char *));
    const char **aliases = malloc(n_src * sizeof(char *));
    char **path_bufs = malloc(n_src * sizeof(char *));
    char **alias_bufs = malloc(n_src * sizeof(char *));

    for (int i = 0; i < n_src; i++) {
        path_bufs[i] = malloc(256);
        alias_bufs[i] = malloc(32);
        snprintf(path_bufs[i], 256, "%s/src_%d.db", BENCH_TMP_DIR, i);
        snprintf(alias_bufs[i], 32, "src_%02d", i);
        paths[i] = path_bufs[i];
        aliases[i] = alias_bufs[i];
        bench_create_source_db(path_bufs[i], 500, 42 + i);
    }
    bench_create_registry(reg_path, paths, aliases, n_src);

    sqlite3 *db = bench_open_vtab(reg_path);
    print_header();

    struct { const char *label; const char *sql; } queries[] = {
        {"no_order",       "SELECT * FROM bench_items"},
        {"single_src_ord", "SELECT * FROM bench_items WHERE _source_db = 'src_00' ORDER BY value"},
        {"multi_src_asc",  "SELECT * FROM bench_items ORDER BY value"},
        {"multi_src_desc", "SELECT * FROM bench_items ORDER BY value DESC"},
        {"order_limit100", "SELECT * FROM bench_items ORDER BY value LIMIT 100"},
    };
    int n_queries = sizeof(queries) / sizeof(queries[0]);

    for (int q = 0; q < n_queries; q++) {
        bench_timer t; timer_init(&t, 100);
        int64_t rows = 0;
        bench_run_query(db, queries[q].sql, BENCH_WARMUP, 50, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row(queries[q].label, &s);
        csv_append("orderby", queries[q].label, &s);
        timer_free(&t);
    }

    sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
    sqlite3_close(db);
    for (int i = 0; i < n_src; i++) { free(path_bufs[i]); free(alias_bufs[i]); }
    free(paths); free(aliases); free(path_bufs); free(alias_bufs);
    bench_cleanup();
}

/* ========== Scenario 6: Row Count Scaling ========== */

static void bench_scenario_row_scale(void)
{
    printf("\n--- Row Count Scaling (1 source) ---\n");

    struct { int rows; int iters; } configs[] = {
        {1000,   200},
        {10000,  50},
        {100000, 10},
        {500000, 3},
    };
    int n_configs = sizeof(configs) / sizeof(configs[0]);

    print_header();

    for (int ci = 0; ci < n_configs; ci++) {
        bench_setup_dir();

        char src_path[256], reg_path[256];
        snprintf(src_path, sizeof(src_path), "%s/src_0.db", BENCH_TMP_DIR);
        snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

        double gen = bench_create_source_db(src_path, configs[ci].rows, 42);
        (void)gen;

        const char *paths[] = {src_path};
        const char *aliases_arr[] = {"src_0"};
        bench_create_registry(reg_path, paths, aliases_arr, 1);

        bench_timer t; timer_init(&t, configs[ci].iters + 10);
        int64_t rows = 0;
        sqlite3 *db = bench_open_vtab(reg_path);
        bench_run_query(db, "SELECT * FROM bench_items", BENCH_WARMUP, configs[ci].iters,
                        &t, &rows);
        sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
        sqlite3_close(db);

        bench_stats s; timer_compute(&t, rows, &s);
        char label[32];
        snprintf(label, sizeof(label), "%dK rows", configs[ci].rows / 1000);
        print_row(label, &s);
        csv_append("row_scale", label, &s);
        timer_free(&t);
        bench_cleanup();
    }
}

/* ========== Scenario 7: Concurrent Readers ========== */

struct concurrent_ctx {
    const char *reg_path;
    const char *query;
    int duration_ms;
    bench_timer timer;
    int64_t total_rows;
    int thread_id;
};

static void *concurrent_worker(void *arg)
{
    struct concurrent_ctx *ctx = (struct concurrent_ctx *)arg;

    sqlite3 *db = bench_open_vtab(ctx->reg_path);

    double deadline = bench_now_us() + (double)ctx->duration_ms * 1000.0;
    while (bench_now_us() < deadline) {
        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(db, ctx->query, -1, &stmt, NULL);
        double t0 = bench_now_us();
        int64_t rows = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int nc = sqlite3_column_count(stmt);
            for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
            rows++;
        }
        double elapsed = bench_now_us() - t0;
        sqlite3_finalize(stmt);
        timer_record(&ctx->timer, elapsed);
        ctx->total_rows += rows;
    }

    sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
    sqlite3_close(db);
    return NULL;
}

static void bench_scenario_concurrent(void)
{
    printf("\n--- Concurrent Readers (10 sources, 1K rows each) ---\n");
    bench_setup_dir();

    int n_src = 10;
    char reg_path[256];
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

    const char *paths[10], *aliases_arr[10];
    char path_bufs[10][256], alias_bufs[10][32];
    for (int i = 0; i < n_src; i++) {
        snprintf(path_bufs[i], sizeof(path_bufs[i]), "%s/src_%d.db", BENCH_TMP_DIR, i);
        snprintf(alias_bufs[i], sizeof(alias_bufs[i]), "src_%02d", i);
        paths[i] = path_bufs[i];
        aliases_arr[i] = alias_bufs[i];
        bench_create_source_db(path_bufs[i], 1000, 42 + i);
    }
    bench_create_registry(reg_path, paths, aliases_arr, n_src);

    int thread_counts[] = {1, 2, 4, 8, 16};
    int n_configs = sizeof(thread_counts) / sizeof(thread_counts[0]);

    print_header();

    for (int ci = 0; ci < n_configs; ci++) {
        int n_threads = thread_counts[ci];
        struct concurrent_ctx *ctxs = calloc(n_threads, sizeof(*ctxs));
        pthread_t *threads = malloc(n_threads * sizeof(pthread_t));

        for (int i = 0; i < n_threads; i++) {
            ctxs[i].reg_path = reg_path;
            ctxs[i].query = "SELECT * FROM bench_items WHERE category = 'cat_042'";
            ctxs[i].duration_ms = 2000;
            ctxs[i].thread_id = i;
            timer_init(&ctxs[i].timer, 2000);
        }

        for (int i = 0; i < n_threads; i++)
            pthread_create(&threads[i], NULL, concurrent_worker, &ctxs[i]);
        for (int i = 0; i < n_threads; i++)
            pthread_join(threads[i], NULL);

        /* Aggregate results */
        int total_queries = 0;
        int64_t total_rows = 0;
        double total_time_us = 0;
        for (int i = 0; i < n_threads; i++) {
            total_queries += ctxs[i].timer.n;
            total_rows += ctxs[i].total_rows;
            for (int j = 0; j < ctxs[i].timer.n; j++)
                total_time_us += ctxs[i].timer.samples[j];
        }

        /* Merge all samples for percentile computation */
        bench_timer merged;
        timer_init(&merged, total_queries + 1);
        for (int i = 0; i < n_threads; i++) {
            for (int j = 0; j < ctxs[i].timer.n; j++)
                timer_record(&merged, ctxs[i].timer.samples[j]);
        }

        bench_stats s;
        timer_compute(&merged, total_rows, &s);
        /* Override rows/s with aggregate throughput */
        s.rows_per_sec = (double)total_rows / 2.0;  /* total rows in 2 seconds */

        char label[32];
        snprintf(label, sizeof(label), "%d threads (%dq)",
                 n_threads, total_queries);
        print_row(label, &s);
        csv_append("concurrent", label, &s);

        timer_free(&merged);
        for (int i = 0; i < n_threads; i++) timer_free(&ctxs[i].timer);
        free(ctxs);
        free(threads);
    }

    bench_cleanup();
}

/* ========== Scenario 8: Federation Showdown ========== */

/*
 * Large-scale comparison: direct SQLite (sequential + parallel) vs clearprism.
 * Default: 100 databases × 1M rows = 100M total rows.
 * Override at compile time: -DFED_N_DBS=200 -DFED_ROWS_PER_DB=5000000
 */
#ifndef FED_N_DBS
#define FED_N_DBS       100
#endif
#ifndef FED_ROWS_PER_DB
#define FED_ROWS_PER_DB 1000000
#endif
#define FED_N_THREADS   16
#define FED_TMP_DIR     "/tmp/clearprism_fed_bench"

/* --- Parallel data generation --- */

struct fed_gen_task {
    char path[256];
    int n_rows;
    uint32_t seed;
};

static struct fed_gen_task *fed_gen_tasks_g;
static int fed_gen_total_g;
static int fed_gen_next_g;

static void *fed_gen_worker(void *arg)
{
    (void)arg;
    while (1) {
        int idx = __sync_fetch_and_add(&fed_gen_next_g, 1);
        if (idx >= fed_gen_total_g) break;
        bench_create_source_db(fed_gen_tasks_g[idx].path,
                                fed_gen_tasks_g[idx].n_rows,
                                fed_gen_tasks_g[idx].seed);
    }
    return NULL;
}

/* Run a query on one pre-opened connection, iterate all rows, return count */
static int64_t fed_run_one(sqlite3 *db, const char *sql)
{
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return 0;
    int64_t rows = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int nc = sqlite3_column_count(stmt);
        for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
        rows++;
    }
    sqlite3_finalize(stmt);
    return rows;
}

/* --- Parallel direct query --- */

struct fed_par_ctx {
    sqlite3 **conns;
    int n_dbs;
    int next_db;  /* atomically incremented */
    const char *sql;
};

struct fed_par_result {
    struct fed_par_ctx *ctx;
    int64_t rows;
};

static void *fed_par_worker(void *arg)
{
    struct fed_par_result *r = (struct fed_par_result *)arg;
    struct fed_par_ctx *ctx = r->ctx;
    int64_t rows = 0;
    while (1) {
        int idx = __sync_fetch_and_add(&ctx->next_db, 1);
        if (idx >= ctx->n_dbs) break;
        rows += fed_run_one(ctx->conns[idx], ctx->sql);
    }
    r->rows = rows;
    return NULL;
}

static void fed_run_parallel(sqlite3 **conns, int n_dbs, const char *sql,
                              int n_threads, int64_t *out_rows, double *out_wall)
{
    struct fed_par_ctx ctx;
    ctx.conns = conns;
    ctx.n_dbs = n_dbs;
    ctx.next_db = 0;
    ctx.sql = sql;

    struct fed_par_result *results = calloc(n_threads, sizeof(*results));
    pthread_t *pt = malloc(n_threads * sizeof(pthread_t));
    for (int i = 0; i < n_threads; i++)
        results[i].ctx = &ctx;

    double t0 = bench_now_us();
    for (int i = 0; i < n_threads; i++)
        pthread_create(&pt[i], NULL, fed_par_worker, &results[i]);
    for (int i = 0; i < n_threads; i++)
        pthread_join(pt[i], NULL);
    *out_wall = bench_now_us() - t0;

    int64_t total = 0;
    for (int i = 0; i < n_threads; i++) total += results[i].rows;
    *out_rows = total;

    free(results);
    free(pt);
}

/* --- Federation output helpers --- */

static void fed_hdr(const char *title)
{
    printf("\n  %s\n", title);
    printf("  %-24s | %10s | %12s | %10s\n",
           "Approach", "Wall Time", "Total Rows", "Rows/sec");
    printf("  %-24s-+-%10s-+-%12s-+-%10s\n",
           "------------------------", "----------", "------------", "----------");
}

static void fed_row(const char *label, double wall_us, int64_t rows,
                    const char *scenario, const char *config)
{
    char wt[16], rate[16];
    fmt_us(wt, sizeof(wt), wall_us);
    double rps = (rows > 0 && wall_us > 0) ? (double)rows / (wall_us / 1e6) : 0;
    fmt_rate(rate, sizeof(rate), rps);
    printf("  %-24s | %10s | %12lld | %10s\n",
           label, wt, (long long)rows, rate);
    bench_stats s = {0};
    s.min_us = wall_us; s.p50_us = wall_us; s.p95_us = wall_us;
    s.p99_us = wall_us; s.max_us = wall_us; s.mean_us = wall_us;
    s.total_rows = rows; s.n_samples = 1; s.rows_per_sec = rps;
    csv_append(scenario, config, &s);
}

/* Parallel scanner callback: touch every column, count rows per thread */
struct fed_scan_par_ctx { int64_t rows; };

static int fed_scan_par_cb(sqlite3_stmt *stmt, int n_cols,
                            const char *source_alias, int thread_id,
                            void *user_ctx)
{
    (void)source_alias;
    struct fed_scan_par_ctx *ctxs = (struct fed_scan_par_ctx *)user_ctx;
    for (int c = 0; c < n_cols; c++)
        sqlite3_column_type(stmt, c + CLEARPRISM_COL_OFFSET);
    ctxs[thread_id].rows++;
    return 0;
}

static void bench_scenario_federation(void)
{
    const int n_dbs = FED_N_DBS;
    const int rows_per = FED_ROWS_PER_DB;
    const int n_threads = FED_N_THREADS;
    int64_t total_expected = (int64_t)n_dbs * rows_per;

    printf("\n=== Federation Showdown (%d DBs x %dM rows = %lldM total) ===\n",
           n_dbs, rows_per / 1000000, (long long)(total_expected / 1000000));

    /* Setup */
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", FED_TMP_DIR);
    (void)system(cmd);
    mkdir(FED_TMP_DIR, 0755);

    /* Allocate path arrays on heap */
    char (*paths)[256] = calloc(n_dbs, sizeof(char[256]));
    char (*alias_arr)[32] = calloc(n_dbs, sizeof(char[32]));
    const char **path_ptrs = malloc(n_dbs * sizeof(char *));
    const char **alias_ptrs = malloc(n_dbs * sizeof(char *));
    char reg_path[256];
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", FED_TMP_DIR);

    for (int i = 0; i < n_dbs; i++) {
        snprintf(paths[i], 256, "%s/src_%03d.db", FED_TMP_DIR, i);
        snprintf(alias_arr[i], 32, "src_%03d", i);
        path_ptrs[i] = paths[i];
        alias_ptrs[i] = alias_arr[i];
    }

    /* Generate one template DB, then copy it n_dbs times */
    printf("  Generating 1 template DB (%dM rows), then copying %d times...\n",
           rows_per / 1000000, n_dbs);
    fflush(stdout);
    double gen_t0 = bench_now_us();

    /* Generate the template */
    char template_path[256];
    snprintf(template_path, sizeof(template_path), "%s/_template.db", FED_TMP_DIR);
    bench_create_source_db(template_path, rows_per, 42);
    printf("  Template generated in %.1fs, copying...\n",
           (bench_now_us() - gen_t0) / 1e6);
    fflush(stdout);

    /* Copy template to all source paths using read/write (no fork overhead) */
    {
        int tfd = open(template_path, O_RDONLY);
        if (tfd >= 0) {
            struct stat st;
            fstat(tfd, &st);
            char *buf = malloc(st.st_size);
            if (buf) {
                ssize_t n = read(tfd, buf, st.st_size);
                for (int i = 0; i < n_dbs; i++) {
                    int dfd = open(paths[i], O_WRONLY | O_CREAT | O_TRUNC, 0644);
                    if (dfd >= 0) {
                        (void)write(dfd, buf, n);
                        close(dfd);
                    }
                }
                free(buf);
            }
            close(tfd);
        }
    }
    unlink(template_path);

    bench_create_registry(reg_path, path_ptrs, alias_ptrs, n_dbs);
    printf("  Generated in %.1fs\n", (bench_now_us() - gen_t0) / 1e6);

    /* Pre-open all connections for direct tests (pooled) */
    sqlite3 **conns = malloc(n_dbs * sizeof(sqlite3 *));
    for (int i = 0; i < n_dbs; i++)
        sqlite3_open_v2(paths[i], &conns[i],
                         SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);

    /* ---- Full Scan ---- */
    {
        char title[128];
        snprintf(title, sizeof(title), "Full Scan (%lldM rows)",
                 (long long)(total_expected / 1000000));
        fed_hdr(title);

        /* Sequential (pre-opened connections) */
        {
            double t0 = bench_now_us();
            int64_t rows = 0;
            for (int i = 0; i < n_dbs; i++)
                rows += fed_run_one(conns[i], "SELECT * FROM items");
            double wall = bench_now_us() - t0;
            fed_row("direct_sequential", wall, rows,
                    "federation_full", "direct_sequential");
        }

        /* Parallel (pre-opened connections, N threads) */
        {
            int64_t rows; double wall;
            fed_run_parallel(conns, n_dbs, "SELECT * FROM items",
                              n_threads, &rows, &wall);
            char label[32];
            snprintf(label, sizeof(label), "direct_%d_threads", n_threads);
            fed_row(label, wall, rows, "federation_full", label);
        }

        /* Clearprism */
        {
            sqlite3 *db;
            sqlite3_open(":memory:", &db);
            clearprism_init(db);
            char *csql = sqlite3_mprintf(
                "CREATE VIRTUAL TABLE fed_items USING clearprism("
                "  registry_db='%s', table='items',"
                "  pool_max_open=%d)", reg_path, n_dbs + 16);
            sqlite3_exec(db, csql, NULL, NULL, NULL);
            sqlite3_free(csql);

            sqlite3_stmt *stmt;
            sqlite3_prepare_v2(db, "SELECT * FROM fed_items", -1, &stmt, NULL);
            double t0 = bench_now_us();
            int64_t rows = 0;
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                int nc = sqlite3_column_count(stmt);
                for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                rows++;
            }
            double wall = bench_now_us() - t0;
            sqlite3_finalize(stmt);
            fed_row("clearprism_vtab", wall, rows,
                    "federation_full", "clearprism_vtab");

            sqlite3_exec(db, "DROP TABLE fed_items", NULL, NULL, NULL);
            sqlite3_close(db);
        }

        /* Clearprism Scanner (zero-vtab-overhead) */
        {
            clearprism_scanner *sc = clearprism_scan_open(reg_path, "items");
            double t0 = bench_now_us();
            int64_t rows = 0;
            int ncol = clearprism_scan_column_count(sc);
            while (clearprism_scan_next(sc)) {
                /* Touch every column to match direct_sequential workload */
                for (int c = 0; c < ncol; c++)
                    clearprism_scan_type(sc, c);
                rows++;
            }
            double wall = bench_now_us() - t0;
            clearprism_scan_close(sc);
            fed_row("clearprism_scanner", wall, rows,
                    "federation_full", "clearprism_scanner");
        }

        /* Clearprism Scanner Parallel (zero-copy, multi-threaded) */
        {
            clearprism_scanner *sc = clearprism_scan_open(reg_path, "items");
            struct fed_scan_par_ctx pctx[FED_N_THREADS];
            memset(pctx, 0, sizeof(pctx));

            double t0 = bench_now_us();
            clearprism_scan_parallel(sc, FED_N_THREADS,
                fed_scan_par_cb, pctx);
            double wall = bench_now_us() - t0;

            int64_t rows = 0;
            for (int i = 0; i < FED_N_THREADS; i++) rows += pctx[i].rows;
            clearprism_scan_close(sc);

            char label[32];
            snprintf(label, sizeof(label), "scanner_%d_threads",
                     FED_N_THREADS);
            fed_row(label, wall, rows,
                    "federation_full", label);
        }
    }

    /* ---- Filtered: category (indexed, ~1% selectivity) ---- */
    {
        fed_hdr("Filtered (WHERE category='cat_042', ~1% selectivity)");
        const char *filt = "SELECT * FROM items WHERE category = 'cat_042'";

        /* Sequential */
        {
            double t0 = bench_now_us();
            int64_t rows = 0;
            for (int i = 0; i < n_dbs; i++)
                rows += fed_run_one(conns[i], filt);
            double wall = bench_now_us() - t0;
            fed_row("direct_sequential", wall, rows,
                    "federation_filt", "direct_sequential");
        }

        /* Parallel */
        {
            int64_t rows; double wall;
            fed_run_parallel(conns, n_dbs, filt, n_threads, &rows, &wall);
            char label[32];
            snprintf(label, sizeof(label), "direct_%d_threads", n_threads);
            fed_row(label, wall, rows, "federation_filt", label);
        }

        /* Clearprism cold + warm */
        {
            sqlite3 *db;
            sqlite3_open(":memory:", &db);
            clearprism_init(db);
            char *csql = sqlite3_mprintf(
                "CREATE VIRTUAL TABLE fed_items USING clearprism("
                "  registry_db='%s', table='items',"
                "  l1_max_rows=2000000, pool_max_open=%d)", reg_path, n_dbs + 16);
            sqlite3_exec(db, csql, NULL, NULL, NULL);
            sqlite3_free(csql);

            /* Cold */
            {
                sqlite3_stmt *stmt;
                sqlite3_prepare_v2(db,
                    "SELECT * FROM fed_items WHERE category = 'cat_042'",
                    -1, &stmt, NULL);
                double t0 = bench_now_us();
                int64_t rows = 0;
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    int nc = sqlite3_column_count(stmt);
                    for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                    rows++;
                }
                double wall = bench_now_us() - t0;
                sqlite3_finalize(stmt);
                fed_row("clearprism_cold", wall, rows,
                        "federation_filt", "clearprism_cold");
            }

            /* Warm (second query — serves from L1 if cached) */
            {
                sqlite3_stmt *stmt;
                sqlite3_prepare_v2(db,
                    "SELECT * FROM fed_items WHERE category = 'cat_042'",
                    -1, &stmt, NULL);
                double t0 = bench_now_us();
                int64_t rows = 0;
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    int nc = sqlite3_column_count(stmt);
                    for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                    rows++;
                }
                double wall = bench_now_us() - t0;
                sqlite3_finalize(stmt);
                fed_row("clearprism_warm", wall, rows,
                        "federation_filt", "clearprism_warm");
            }

            sqlite3_exec(db, "DROP TABLE fed_items", NULL, NULL, NULL);
            sqlite3_close(db);
        }
    }

    /* ---- Point Lookup (PK, 1 row per DB) ---- */
    {
        fed_hdr("Point Lookup (WHERE id=500, 1 row per DB)");
        const char *point = "SELECT * FROM items WHERE id = 500";

        /* Sequential */
        {
            double t0 = bench_now_us();
            int64_t rows = 0;
            for (int i = 0; i < n_dbs; i++)
                rows += fed_run_one(conns[i], point);
            double wall = bench_now_us() - t0;
            fed_row("direct_sequential", wall, rows,
                    "federation_point", "direct_sequential");
        }

        /* Parallel */
        {
            int64_t rows; double wall;
            fed_run_parallel(conns, n_dbs, point, n_threads, &rows, &wall);
            char label[32];
            snprintf(label, sizeof(label), "direct_%d_threads", n_threads);
            fed_row(label, wall, rows, "federation_point", label);
        }

        /* Clearprism cold + warm */
        {
            sqlite3 *db;
            sqlite3_open(":memory:", &db);
            clearprism_init(db);
            char *csql = sqlite3_mprintf(
                "CREATE VIRTUAL TABLE fed_items USING clearprism("
                "  registry_db='%s', table='items',"
                "  l1_max_rows=100000, pool_max_open=%d)", reg_path, n_dbs + 16);
            sqlite3_exec(db, csql, NULL, NULL, NULL);
            sqlite3_free(csql);

            /* Cold */
            {
                sqlite3_stmt *stmt;
                sqlite3_prepare_v2(db,
                    "SELECT * FROM fed_items WHERE id = 500",
                    -1, &stmt, NULL);
                double t0 = bench_now_us();
                int64_t rows = 0;
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    int nc = sqlite3_column_count(stmt);
                    for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                    rows++;
                }
                double wall = bench_now_us() - t0;
                sqlite3_finalize(stmt);
                fed_row("clearprism_cold", wall, rows,
                        "federation_point", "clearprism_cold");
            }

            /* Warm (cached — 100 rows fits easily in L1) */
            {
                sqlite3_stmt *stmt;
                sqlite3_prepare_v2(db,
                    "SELECT * FROM fed_items WHERE id = 500",
                    -1, &stmt, NULL);
                double t0 = bench_now_us();
                int64_t rows = 0;
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    int nc = sqlite3_column_count(stmt);
                    for (int c = 0; c < nc; c++) sqlite3_column_value(stmt, c);
                    rows++;
                }
                double wall = bench_now_us() - t0;
                sqlite3_finalize(stmt);
                fed_row("clearprism_warm", wall, rows,
                        "federation_point", "clearprism_warm");
            }

            sqlite3_exec(db, "DROP TABLE fed_items", NULL, NULL, NULL);
            sqlite3_close(db);
        }
    }

    /* Cleanup */
    for (int i = 0; i < n_dbs; i++) sqlite3_close(conns[i]);
    free(conns);
    free(paths);
    free(alias_arr);
    free(path_ptrs);
    free(alias_ptrs);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", FED_TMP_DIR);
    (void)system(cmd);
}

/* ========== Scenario 9: Parallel Drain ========== */

static void bench_scenario_drain(void)
{
    printf("\n--- Parallel Drain (10 sources, 1K rows each, filtered) ---\n");
    bench_setup_dir();

    int n_src = 10;
    char reg_path[256];
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

    const char *paths[10], *aliases[10];
    char path_bufs[10][256], alias_bufs[10][32];
    for (int i = 0; i < n_src; i++) {
        snprintf(path_bufs[i], sizeof(path_bufs[i]), "%s/src_%d.db", BENCH_TMP_DIR, i);
        snprintf(alias_bufs[i], sizeof(alias_bufs[i]), "src_%02d", i);
        paths[i] = path_bufs[i];
        aliases[i] = alias_bufs[i];
        bench_create_source_db(path_bufs[i], 1000, 42 + i);
    }
    bench_create_registry(reg_path, paths, aliases, n_src);

    sqlite3 *db = bench_open_vtab(reg_path);
    print_header();

    /* Selective filtered query — exercises parallel drain */
    struct { const char *label; const char *sql; } queries[] = {
        {"drain_eq_cold",   "SELECT * FROM bench_items WHERE category = 'cat_042'"},
        {"drain_range",     "SELECT * FROM bench_items WHERE value BETWEEN 500 AND 1000"},
        {"drain_limit50",   "SELECT * FROM bench_items WHERE category = 'cat_010' LIMIT 50"},
        {"drain_order",     "SELECT * FROM bench_items WHERE category = 'cat_005' ORDER BY value"},
    };
    int n_queries = sizeof(queries) / sizeof(queries[0]);

    for (int q = 0; q < n_queries; q++) {
        /* Use fresh vtab for each to test cold drain */
        sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
        sqlite3_close(db);
        db = bench_open_vtab(reg_path);

        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db, queries[q].sql, 1, 50, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row(queries[q].label, &s);
        csv_append("drain", queries[q].label, &s);
        timer_free(&t);
    }

    sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
    sqlite3_close(db);
    bench_cleanup();
}

/* ========== Scenario 10: Aggregate Pushdown ========== */

static void bench_scenario_agg_pushdown(void)
{
    printf("\n--- Aggregate Pushdown (10 sources, 10K rows each) ---\n");
    bench_setup_dir();

    int n_src = 10;
    char reg_path[256];
    snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

    const char *p[10], *a[10];
    char pb[10][256], ab[10][32];
    for (int i = 0; i < n_src; i++) {
        snprintf(pb[i], sizeof(pb[i]), "%s/src_%d.db", BENCH_TMP_DIR, i);
        snprintf(ab[i], sizeof(ab[i]), "src_%02d", i);
        p[i] = pb[i]; a[i] = ab[i];
        bench_create_source_db(pb[i], 10000, 42 + i);
    }
    bench_create_registry(reg_path, p, a, n_src);

    sqlite3 *db = bench_open_vtab(reg_path);
    print_header();

    /* Regular COUNT(*) via vtab iteration */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db, "SELECT COUNT(*) FROM bench_items", 1, 20, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("vtab_count_star", &s);
        csv_append("agg_pushdown", "vtab_count_star", &s);
        timer_free(&t);
    }

    /* Pushdown COUNT via clearprism_count */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db, "SELECT clearprism_count('items', NULL)", 1, 100, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("pushdown_count", &s);
        csv_append("agg_pushdown", "pushdown_count", &s);
        timer_free(&t);
    }

    /* Regular SUM via vtab iteration */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db, "SELECT SUM(value) FROM bench_items", 1, 20, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("vtab_sum", &s);
        csv_append("agg_pushdown", "vtab_sum", &s);
        timer_free(&t);
    }

    /* Pushdown SUM */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db, "SELECT clearprism_sum('items', 'value', NULL)", 1, 100, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("pushdown_sum", &s);
        csv_append("agg_pushdown", "pushdown_sum", &s);
        timer_free(&t);
    }

    /* Pushdown with WHERE */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db,
            "SELECT clearprism_count('items', 'category = ''cat_042''')",
            1, 100, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("pushdown_cnt_where", &s);
        csv_append("agg_pushdown", "pushdown_cnt_where", &s);
        timer_free(&t);
    }

    /* Pushdown AVG */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db,
            "SELECT clearprism_avg('items', 'value', NULL)",
            1, 100, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("pushdown_avg", &s);
        csv_append("agg_pushdown", "pushdown_avg", &s);
        timer_free(&t);
    }

    /* Pushdown MIN/MAX */
    {
        bench_timer t; timer_init(&t, 200);
        int64_t rows = 0;
        bench_run_query(db,
            "SELECT clearprism_min('items', 'value', NULL)",
            1, 100, &t, &rows);
        bench_stats s; timer_compute(&t, rows, &s);
        print_row("pushdown_min", &s);
        csv_append("agg_pushdown", "pushdown_min", &s);
        timer_free(&t);
    }

    sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
    sqlite3_close(db);
    bench_cleanup();
}

/* ========== Snapshot population ========== */

static void bench_scenario_snapshot(void)
{
    struct { int n_src; int rows_per; const char *label; } configs[] = {
        {10,  1000,  "10x1K"},
        {10,  10000, "10x10K"},
        {100, 1000,  "100x1K"},
        {1000, 1000, "1000x1K"},
    };
    int n_configs = (int)(sizeof(configs) / sizeof(configs[0]));

    for (int ci = 0; ci < n_configs; ci++) {
        int n_src = configs[ci].n_src;
        int rows_per = configs[ci].rows_per;

        printf("\n--- Snapshot Population: %s (%dK total rows) ---\n",
               configs[ci].label, n_src * rows_per / 1000);
        bench_setup_dir();

        char reg_path[256];
        snprintf(reg_path, sizeof(reg_path), "%s/registry.db", BENCH_TMP_DIR);

        const char **paths = malloc(n_src * sizeof(char *));
        const char **aliases = malloc(n_src * sizeof(char *));
        char **path_bufs = malloc(n_src * sizeof(char *));
        char **alias_bufs = malloc(n_src * sizeof(char *));

        for (int i = 0; i < n_src; i++) {
            path_bufs[i] = malloc(256);
            alias_bufs[i] = malloc(32);
            snprintf(path_bufs[i], 256, "%s/src_%d.db", BENCH_TMP_DIR, i);
            snprintf(alias_bufs[i], 32, "src_%03d", i);
            paths[i] = path_bufs[i];
            aliases[i] = alias_bufs[i];
            bench_create_source_db(path_bufs[i], rows_per, 42 + i);
        }
        bench_create_registry(reg_path, paths, aliases, n_src);

        print_header();

        {
            bench_timer t; timer_init(&t, 30);
            int64_t total_rows = 0;

            /* Warmup */
            {
                sqlite3 *db;
                sqlite3_open(":memory:", &db);
                clearprism_init(db);
                char *sql = sqlite3_mprintf(
                    "CREATE VIRTUAL TABLE bench_items USING clearprism("
                    "  registry_db='%s', table='items', mode='snapshot')", reg_path);
                sqlite3_exec(db, sql, NULL, NULL, NULL);
                sqlite3_free(sql);
                sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
                sqlite3_close(db);
            }

            int iters = (n_src * rows_per > 100000) ? 3 : 10;
            for (int i = 0; i < iters; i++) {
                sqlite3 *db;
                sqlite3_open(":memory:", &db);
                clearprism_init(db);
                char *sql = sqlite3_mprintf(
                    "CREATE VIRTUAL TABLE bench_items USING clearprism("
                    "  registry_db='%s', table='items', mode='snapshot')", reg_path);
                double t0 = bench_now_us();
                sqlite3_exec(db, sql, NULL, NULL, NULL);
                double elapsed = bench_now_us() - t0;
                sqlite3_free(sql);

                sqlite3_stmt *stmt;
                sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM bench_items",
                                   -1, &stmt, NULL);
                int64_t rows = 0;
                if (sqlite3_step(stmt) == SQLITE_ROW)
                    rows = sqlite3_column_int64(stmt, 0);
                sqlite3_finalize(stmt);
                total_rows += rows;

                timer_record(&t, elapsed);
                sqlite3_exec(db, "DROP TABLE bench_items", NULL, NULL, NULL);
                sqlite3_close(db);
            }

            bench_stats s; timer_compute(&t, total_rows, &s);
            print_row("direct_open", &s);
            csv_append("snapshot", "direct_open", &s);
            timer_free(&t);
        }

        /* Scanner API: the old approach using clearprism_scan_* with
         * sqlite3_value_dup + sqlite3_bind_value per cell */
        {
            bench_timer t; timer_init(&t, 30);
            int64_t total_rows = 0;

            /* Warmup */
            {
                sqlite3 *db;
                sqlite3_open(":memory:", &db);
                sqlite3_exec(db,
                    "CREATE TABLE _snap(id INTEGER, category TEXT, name TEXT,"
                    " value REAL, description TEXT, created_at TEXT,"
                    " _source_db TEXT)", NULL, NULL, NULL);
                sqlite3_stmt *ins;
                sqlite3_prepare_v2(db,
                    "INSERT INTO _snap(rowid,id,category,name,value,"
                    "description,created_at,_source_db) VALUES(?,?,?,?,?,?,?,?)",
                    -1, &ins, NULL);
                clearprism_scanner *sc = clearprism_scan_open(reg_path, "items");
                if (sc) {
                    while (clearprism_scan_next(sc)) {
                        int64_t sid = clearprism_scan_source_id(sc);
                        int64_t rid = clearprism_scan_rowid(sc);
                        sqlite3_bind_int64(ins, 1,
                            (sid << CLEARPRISM_ROWID_SHIFT) |
                            (rid & CLEARPRISM_ROWID_MASK));
                        for (int c = 0; c < 6; c++) {
                            sqlite3_value *v = clearprism_scan_value(sc, c);
                            if (v) sqlite3_bind_value(ins, c + 2, v);
                            else   sqlite3_bind_null(ins, c + 2);
                        }
                        const char *a = clearprism_scan_source_alias(sc);
                        if (a) sqlite3_bind_text(ins, 8, a, -1, SQLITE_TRANSIENT);
                        else   sqlite3_bind_null(ins, 8);
                        sqlite3_step(ins);
                        sqlite3_reset(ins);
                    }
                    clearprism_scan_close(sc);
                }
                sqlite3_finalize(ins);
                sqlite3_exec(db, "DROP TABLE _snap", NULL, NULL, NULL);
                sqlite3_close(db);
            }

            int iters = (n_src * rows_per > 100000) ? 3 : 10;
            for (int i = 0; i < iters; i++) {
                sqlite3 *db;
                sqlite3_open(":memory:", &db);
                sqlite3_exec(db,
                    "CREATE TABLE _snap(id INTEGER, category TEXT, name TEXT,"
                    " value REAL, description TEXT, created_at TEXT,"
                    " _source_db TEXT)", NULL, NULL, NULL);
                sqlite3_stmt *ins;
                sqlite3_prepare_v2(db,
                    "INSERT INTO _snap(rowid,id,category,name,value,"
                    "description,created_at,_source_db) VALUES(?,?,?,?,?,?,?,?)",
                    -1, &ins, NULL);

                double t0 = bench_now_us();
                clearprism_scanner *sc = clearprism_scan_open(reg_path, "items");
                if (sc) {
                    while (clearprism_scan_next(sc)) {
                        int64_t sid = clearprism_scan_source_id(sc);
                        int64_t rid = clearprism_scan_rowid(sc);
                        sqlite3_bind_int64(ins, 1,
                            (sid << CLEARPRISM_ROWID_SHIFT) |
                            (rid & CLEARPRISM_ROWID_MASK));
                        for (int c = 0; c < 6; c++) {
                            sqlite3_value *v = clearprism_scan_value(sc, c);
                            if (v) sqlite3_bind_value(ins, c + 2, v);
                            else   sqlite3_bind_null(ins, c + 2);
                        }
                        const char *a = clearprism_scan_source_alias(sc);
                        if (a) sqlite3_bind_text(ins, 8, a, -1, SQLITE_TRANSIENT);
                        else   sqlite3_bind_null(ins, 8);
                        sqlite3_step(ins);
                        sqlite3_reset(ins);
                    }
                    clearprism_scan_close(sc);
                }
                double elapsed = bench_now_us() - t0;

                sqlite3_stmt *cnt;
                sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM _snap",
                                   -1, &cnt, NULL);
                int64_t rows = 0;
                if (sqlite3_step(cnt) == SQLITE_ROW)
                    rows = sqlite3_column_int64(cnt, 0);
                sqlite3_finalize(cnt);
                total_rows += rows;

                timer_record(&t, elapsed);
                sqlite3_finalize(ins);
                sqlite3_exec(db, "DROP TABLE _snap", NULL, NULL, NULL);
                sqlite3_close(db);
            }

            bench_stats s; timer_compute(&t, total_rows, &s);
            print_row("scanner_api", &s);
            csv_append("snapshot", "scanner_api", &s);
            timer_free(&t);
        }

        for (int i = 0; i < n_src; i++) { free(path_bufs[i]); free(alias_bufs[i]); }
        free(paths); free(aliases); free(path_bufs); free(alias_bufs);
        bench_cleanup();
    }
}

/* ========== Main ========== */

int main(int argc, char **argv)
{
    const char *selected = argc > 1 ? argv[1] : NULL;

    /* Cap virtual memory at 8 GB to prevent OOM crashes */
    {
        struct rlimit rl;
        rl.rlim_cur = (rlim_t)8ULL * 1024 * 1024 * 1024;
        rl.rlim_max = (rlim_t)8ULL * 1024 * 1024 * 1024;
        setrlimit(RLIMIT_AS, &rl);
    }

    printf("=== Clearprism Benchmark Suite ===\n");
    printf("Version: %s\n", CLEARPRISM_VERSION_STRING);
    printf("SQLite: %s\n", sqlite3_libversion());
    printf("Max prepare threads: %d\n", CLEARPRISM_MAX_PREPARE_THREADS);

    struct { const char *name; void (*fn)(void); } scenarios[] = {
        {"baseline",      bench_scenario_baseline},
        {"source_scale",  bench_scenario_source_scale},
        {"cache",         bench_scenario_cache},
        {"where",         bench_scenario_where},
        {"orderby",       bench_scenario_orderby},
        {"row_scale",     bench_scenario_row_scale},
        {"concurrent",    bench_scenario_concurrent},
        {"federation",    bench_scenario_federation},
        {"drain",         bench_scenario_drain},
        {"agg_pushdown",  bench_scenario_agg_pushdown},
        {"snapshot",      bench_scenario_snapshot},
    };
    int n_scenarios = (int)(sizeof(scenarios) / sizeof(scenarios[0]));

    int ran = 0;
    for (int i = 0; i < n_scenarios; i++) {
        if (selected && strcmp(selected, scenarios[i].name) != 0) continue;
        scenarios[i].fn();
        ran++;
    }

    if (selected && ran == 0) {
        printf("\nUnknown scenario: '%s'\n", selected);
        printf("Available: ");
        for (int i = 0; i < n_scenarios; i++)
            printf("%s%s", scenarios[i].name, i < n_scenarios - 1 ? ", " : "\n");
        return 1;
    }

    printf("\n=== Benchmark complete ===\n");
    printf("CSV results appended to %s\n", BENCH_CSV_FILE);
    return 0;
}
