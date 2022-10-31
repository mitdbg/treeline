cmake_minimum_required(VERSION 3.17)
project(treeline LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 17)

option(TL_BUILD_TESTS "Set to build the TreeLine test suite." OFF)
option(TL_BUILD_BENCHMARKS "Set to build the TreeLine benchmarks." OFF)
option(TL_BUILD_SHARED "Set to build TreeLine as a shared library." OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")  # Needed for ALEX

# Allows us to set CMake project options for subprojects that we include.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_library(tbb tbb)

# The TreeLine embedded database library targets
if (TL_BUILD_SHARED)
  add_library(treeline SHARED)
  add_library(pg_treeline SHARED)
else()
  add_library(treeline STATIC)
  add_library(pg_treeline STATIC)
endif()

# Load any third party dependencies
add_subdirectory(third_party)

target_link_libraries(treeline PRIVATE crc32c tbb masstree)
target_link_libraries(pg_treeline PRIVATE crc32c tbb masstree)
target_include_directories(treeline
  PUBLIC include
  PRIVATE .)
target_include_directories(pg_treeline
  PUBLIC include
  PRIVATE .)

# Add API headers to the target for IDE support
set(treeline_inc include/treeline)
set(treeline_api
  ${treeline_inc}/db.h
  ${treeline_inc}/options.h
  ${treeline_inc}/record_batch.h
  ${treeline_inc}/slice.h
  ${treeline_inc}/statistics.h
  ${treeline_inc}/status.h
)
target_sources(treeline PUBLIC ${treeline_api})
set(pg_treeline_api
  ${treeline_inc}/pg_db.h
  ${treeline_inc}/pg_options.h
  ${treeline_inc}/pg_stats.h
  ${treeline_inc}/slice.h
  ${treeline_inc}/status.h
)
target_sources(pg_treeline PUBLIC ${pg_treeline_api})

# Add our sources by traversing the repository
add_subdirectory(bufmgr)
add_subdirectory(db)
add_subdirectory(model)
add_subdirectory(page_grouping)
add_subdirectory(record_cache)
add_subdirectory(util)
add_subdirectory(wal)

# Build the tests iff requested
if(TL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# Build the benchmarks iff requested
if(TL_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()
