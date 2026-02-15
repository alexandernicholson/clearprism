# Building

Clearprism supports two build systems: GNU Make and CMake.

## Prerequisites

- **C compiler**: GCC or Clang with C11 support
- **SQLite3**: Development headers and library (`libsqlite3-dev` on Debian/Ubuntu, `sqlite-devel` on Fedora/RHEL)
- **pthreads**: Standard on Linux and macOS

### Install dependencies

**Debian / Ubuntu:**
```bash
sudo apt install build-essential libsqlite3-dev
```

**Fedora / RHEL:**
```bash
sudo dnf install gcc sqlite-devel
```

**macOS (Homebrew):**
```bash
brew install sqlite3
```

## Building with Make

```bash
cd clearprism
make
```

This produces `clearprism.so` on Linux or `clearprism.dylib` on macOS.

### Make targets

| Target | Description |
|--------|-------------|
| `make` / `make all` | Build the shared library |
| `make test` | Build and run the test suite |
| `make bench` | Build and run the benchmark suite |
| `make clean` | Remove all build artifacts |

### Customizing the compiler

```bash
make CC=clang
```

## Building with CMake

```bash
cd clearprism
mkdir build && cd build
cmake ..
make
```

### CMake targets

| Target | Description |
|--------|-------------|
| `make` | Build the shared library and test executable |
| `make test_run` | Build and run the test suite |
| `ctest` | Run tests via CTest |

### CMake options

The build uses `pkg-config` to find SQLite3. If SQLite3 is installed in a non-standard location, set the `CMAKE_PREFIX_PATH`:

```bash
cmake -DCMAKE_PREFIX_PATH=/opt/sqlite3 ..
```

## Build Configurations

### Loadable extension (default)

Built with `-DSQLITE_CORE=0`. Produces a shared library that can be loaded with:

```sql
.load ./clearprism
```

or programmatically:

```c
sqlite3_load_extension(db, "./clearprism", NULL, &errmsg);
```

### Core compilation

Built with `-DSQLITE_CORE=1`. For linking directly into a custom SQLite build. Exposes:

```c
int clearprism_init(sqlite3 *db);
```

### Test build

Built with `-DCLEARPRISM_TESTING=1 -DSQLITE_CORE=1`. Compiles all source files with the test runner. Links against SQLite as a regular library (not as an extension).

## Output Files

| File | Description |
|------|-------------|
| `clearprism.so` / `clearprism.dylib` | Loadable extension shared library |
| `clearprism_tests` | Test executable |
| `clearprism_bench` | Benchmark executable |
| `src/*.o` | Object files (Make build) |
| `src/*-test.o` | Test-mode object files (Make build) |

## Verifying the Build

```bash
# Run the test suite
make test

# Or load interactively
sqlite3
> .load ./clearprism
> -- If no error, the extension loaded successfully
```

## Compiler Flags

Both build systems use:

| Flag | Purpose |
|------|---------|
| `-std=c11` | C11 standard |
| `-Wall -Wextra` | Comprehensive warnings |
| `-Wno-unused-parameter` | SQLite callbacks have unused parameters by convention |
| `-fPIC` | Position-independent code (required for shared libraries) |
| `-O2` | Optimization (Make only; CMake defaults vary by build type) |

## Cross-Platform Notes

- **Linux**: Produces `clearprism.so`. Uses `-shared` linker flag.
- **macOS**: Produces `clearprism.dylib`. Uses `-dynamiclib` linker flag.
- **Windows**: The extension declares `__declspec(dllexport)` on the entry point. Windows builds are not yet tested but the source is structured to support them.
