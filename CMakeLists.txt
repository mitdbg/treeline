cmake_minimum_required(VERSION 3.17)
project(learnedlsm LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

option(LLSM_BUILD_TESTS "Set to build the Learned LSM test suite." OFF)
option(LLSM_BUILD_BENCHMARKS "Set to build the Learned LSM benchmarks." OFF)
option(LLSM_BUILD_SHARED "Set to build the Learned LSM database as a shared library." OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")  # Needed for ALEX

# Allows us to set CMake project options for subprojects that we include.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Load any third party dependencies
add_subdirectory(third_party)

# The Learned LSM embedded database library target
if (LLSM_BUILD_SHARED)
  add_library(llsm SHARED)
else()
  add_library(llsm STATIC)
endif()
target_link_libraries(llsm crc32c)
target_include_directories(llsm
  PUBLIC include
  PRIVATE .)

# Add API headers to the target for IDE support
set(llsm_inc include/llsm)
set(llsm_api
  ${llsm_inc}/db.h
  ${llsm_inc}/options.h
  ${llsm_inc}/slice.h
  ${llsm_inc}/status.h)
target_sources(llsm PUBLIC ${llsm_api})

# Add our sources by traversing the repository
add_subdirectory(alex)
add_subdirectory(art_olc)
add_subdirectory(bufmgr)
add_subdirectory(db)
add_subdirectory(model)
add_subdirectory(record_cache)
add_subdirectory(tlx)
add_subdirectory(util)
add_subdirectory(wal)

# Build the tests iff requested
if(LLSM_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# Build the benchmarks iff requested
if(LLSM_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()
