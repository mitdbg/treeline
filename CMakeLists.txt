cmake_minimum_required(VERSION 3.17)
project(learnedlsm LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 17)

option(LLSM_BUILD_TESTS "Set to build the Learned LSM test suite." OFF)
option(LLSM_BUILD_BENCHMARKS "Set to build the Learned LSM benchmarks." OFF)
option(LLSM_BUILD_SHARED "Set to build the Learned LSM database as a shared library." OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")  # Needed for ALEX

# Allows us to set CMake project options for subprojects that we include.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_library(tbb tbb)

# Load any third party dependencies
add_subdirectory(third_party)

# The Learned LSM embedded database library targets
if (LLSM_BUILD_SHARED)
  add_library(tl SHARED)
  add_library(pg_tl SHARED)
else()
  add_library(tl STATIC)
  add_library(pg_tl STATIC)
endif()
target_link_libraries(tl PRIVATE crc32c tbb masstree)
target_link_libraries(pg_tl PRIVATE crc32c tbb masstree)
target_include_directories(tl
  PUBLIC include
  PRIVATE .)
target_include_directories(pg_tl
  PUBLIC include
  PRIVATE .)

# Add API headers to the target for IDE support
set(tl_inc include/tl)
set(tl_api
  ${tl_inc}/db.h
  ${tl_inc}/options.h
  ${tl_inc}/record_batch.h
  ${tl_inc}/slice.h
  ${tl_inc}/statistics.h
  ${tl_inc}/status.h
)
target_sources(tl PUBLIC ${tl_api})
set(pg_tl_api
  ${tl_inc}/pg_db.h
  ${tl_inc}/pg_options.h
  ${tl_inc}/pg_stats.h
  ${tl_inc}/slice.h
  ${tl_inc}/status.h
)
target_sources(pg_tl PUBLIC ${pg_tl_api})

# Add our sources by traversing the repository
add_subdirectory(alex)
add_subdirectory(art_olc)
add_subdirectory(bufmgr)
add_subdirectory(db)
add_subdirectory(masstree)
add_subdirectory(masstree_wrapper)
add_subdirectory(model)
add_subdirectory(record_cache)
add_subdirectory(tlx)
add_subdirectory(util)
add_subdirectory(wal)

add_subdirectory(page_grouping)

# Build the tests iff requested
if(LLSM_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# Build the benchmarks iff requested
if(LLSM_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()
