include(FetchContent)

if(LLSM_BUILD_TESTS)
  # Google Tests: Fetch it iff we need to build the test suite
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        release-1.10.0
  )
  FetchContent_MakeAvailable(googletest)
endif()

if(LLSM_BUILD_BENCHMARKS)
  FetchContent_Declare(
    gflags
    GIT_REPOSITORY https://github.com/gflags/gflags
    GIT_TAG        v2.2.2
  )
  FetchContent_MakeAvailable(gflags)

  FetchContent_Declare(
    rocksdb
    GIT_REPOSITORY https://github.com/facebook/rocksdb
    GIT_TAG        v6.14.6
  )
  set(WITH_GFLAGS OFF)
  set(WITH_JNI OFF)
  set(WITH_TESTS OFF)
  set(WITH_BENCHMARK_TOOLS OFF)
  set(WITH_CORE_TOOLS OFF)
  set(WITH_TOOLS OFF)
  FetchContent_MakeAvailable(rocksdb)

  # The RocksDB CMakeLists.txt exports RocksDB as a static library called
  # "rocksdb". However they do not specify the path to the headers properly.
  # This target_include_directories() call below lets us link to RocksDB as if
  # it were a subproject within our project.
  target_include_directories(rocksdb
    PUBLIC $<BUILD_INTERFACE:${rocksdb_SOURCE_DIR}/include>)
endif()
