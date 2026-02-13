cmake_minimum_required(VERSION 3.10)
project(clearprism VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find required libraries
find_package(Threads REQUIRED)

# SQLite3 - try pkg-config first, then fallback
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(SQLITE3 QUIET sqlite3)
endif()
if(NOT SQLITE3_FOUND)
    find_library(SQLITE3_LIBRARIES NAMES sqlite3)
    find_path(SQLITE3_INCLUDE_DIRS NAMES sqlite3.h)
    if(NOT SQLITE3_LIBRARIES OR NOT SQLITE3_INCLUDE_DIRS)
        message(FATAL_ERROR "SQLite3 not found. Install libsqlite3-dev.")
    endif()
endif()

# Source files
set(CLEARPRISM_SOURCES
    src/clearprism_main.c
    src/clearprism_vtab.c
    src/clearprism_query.c
    src/clearprism_registry.c
    src/clearprism_connpool.c
    src/clearprism_cache_l1.c
    src/clearprism_cache_l2.c
    src/clearprism_cache.c
    src/clearprism_where.c
    src/clearprism_util.c
)

# Shared library (loadable extension)
add_library(clearprism SHARED ${CLEARPRISM_SOURCES})
target_include_directories(clearprism PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${SQLITE3_INCLUDE_DIRS}
)
target_link_libraries(clearprism PRIVATE
    ${SQLITE3_LIBRARIES}
    Threads::Threads
)
target_compile_definitions(clearprism PRIVATE
    SQLITE_CORE=0
)
target_compile_options(clearprism PRIVATE -Wall -Wextra -Wno-unused-parameter)

# Remove "lib" prefix for the shared library
set_target_properties(clearprism PROPERTIES PREFIX "")

# Test executable
set(TEST_SOURCES
    test/test_main.c
    test/test_vtab.c
    test/test_cache.c
    test/test_connpool.c
    test/test_registry.c
)

add_executable(clearprism_tests
    ${TEST_SOURCES}
    ${CLEARPRISM_SOURCES}
)
target_include_directories(clearprism_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${SQLITE3_INCLUDE_DIRS}
)
target_link_libraries(clearprism_tests PRIVATE
    ${SQLITE3_LIBRARIES}
    Threads::Threads
)
target_compile_definitions(clearprism_tests PRIVATE
    CLEARPRISM_TESTING=1
    SQLITE_CORE=1
)
target_compile_options(clearprism_tests PRIVATE -Wall -Wextra -Wno-unused-parameter)

# CTest support
enable_testing()
add_test(NAME clearprism_tests COMMAND clearprism_tests)

# Custom test target
add_custom_target(test_run
    COMMAND clearprism_tests
    DEPENDS clearprism_tests
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running clearprism tests"
)
