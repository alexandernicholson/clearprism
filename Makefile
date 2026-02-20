CC ?= gcc
CFLAGS = -std=c11 -Wall -Wextra -Wno-unused-parameter -fPIC -O2
LDFLAGS = -lsqlite3 -lpthread

INCLUDES = -Iinclude

# Detect OS for shared library suffix
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	SHARED_EXT = .dylib
	SHARED_FLAGS = -dynamiclib
else
	SHARED_EXT = .so
	SHARED_FLAGS = -shared
endif

# Source files
SRCS = src/clearprism_main.c \
       src/clearprism_vtab.c \
       src/clearprism_query.c \
       src/clearprism_registry.c \
       src/clearprism_connpool.c \
       src/clearprism_cache_l1.c \
       src/clearprism_cache_l2.c \
       src/clearprism_cache.c \
       src/clearprism_where.c \
       src/clearprism_util.c \
       src/clearprism_agg.c \
       src/clearprism_scanner.c \
       src/clearprism_admin.c

TEST_SRCS = test/test_main.c \
            test/test_vtab.c \
            test/test_cache.c \
            test/test_connpool.c \
            test/test_registry.c \
            test/test_agg.c \
            test/test_scanner.c \
            test/test_admin.c

BENCH_SRCS = bench/bench_main.c

OBJS = $(SRCS:.c=.o)
TEST_OBJS = $(TEST_SRCS:.c=.o)
BENCH_OBJS = $(BENCH_SRCS:.c=.o)

# Targets
TARGET = clearprism$(SHARED_EXT)
TEST_TARGET = clearprism_tests
BENCH_TARGET = clearprism_bench

.PHONY: all clean test bench

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(SHARED_FLAGS) -o $@ $^ $(LDFLAGS)

src/%.o: src/%.c include/clearprism.h
	$(CC) $(CFLAGS) $(INCLUDES) -DSQLITE_CORE=0 -c -o $@ $<

# Test build - compile source files with CLEARPRISM_TESTING and SQLITE_CORE
test/%.o: test/%.c include/clearprism.h
	$(CC) $(CFLAGS) $(INCLUDES) -Isrc -DCLEARPRISM_TESTING=1 -DSQLITE_CORE=1 -c -o $@ $<

# Recompile source files for testing (with SQLITE_CORE=1)
SRCS_TEST_OBJS = $(patsubst src/%.c,src/%-test.o,$(SRCS))

src/%-test.o: src/%.c include/clearprism.h
	$(CC) $(CFLAGS) $(INCLUDES) -DCLEARPRISM_TESTING=1 -DSQLITE_CORE=1 -c -o $@ $<

$(TEST_TARGET): $(SRCS_TEST_OBJS) $(TEST_OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

test: $(TEST_TARGET)
	./$(TEST_TARGET)

# Bench build - compile source files with SQLITE_CORE for static linking
bench/%.o: bench/%.c include/clearprism.h
	$(CC) $(CFLAGS) $(INCLUDES) -DSQLITE_CORE=1 -c -o $@ $<

$(BENCH_TARGET): $(SRCS_TEST_OBJS) $(BENCH_OBJS)
	$(CC) -o $@ $^ $(LDFLAGS) -lm

bench: $(BENCH_TARGET)
	./$(BENCH_TARGET)

bench-quick: $(BENCH_TARGET)
	./$(BENCH_TARGET) quick

clean:
	rm -f src/*.o src/*-test.o test/*.o bench/*.o $(TARGET) $(TEST_TARGET) $(BENCH_TARGET)
