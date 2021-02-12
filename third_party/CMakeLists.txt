include(FetchContent)

# CRC32C implemented using CPU-specific instructions
FetchContent_Declare(
  crc32c
  GIT_REPOSITORY https://github.com/google/crc32c
  GIT_TAG        1.1.1
)
set(CRC32C_BUILD_TESTS OFF)
set(CRC32C_BUILD_BENCHMARKS OFF)
set(CRC32C_USE_GLOG OFF)
set(CRC32C_INSTALL OFF)
FetchContent_MakeAvailable(crc32c)

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
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark
    GIT_TAG        v1.5.2
  )
  set(BENCHMARK_ENABLE_TESTING OFF)
  set(BENCHMARK_ENABLE_INSTALL OFF)
  FetchContent_MakeAvailable(benchmark)

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
  set(ROCKSDB_BUILD_SHARED OFF)
  FetchContent_MakeAvailable(rocksdb)

  # The RocksDB CMakeLists.txt exports RocksDB as a static library called
  # "rocksdb". However they do not specify the path to the headers properly.
  # This target_include_directories() call below lets us link to RocksDB as if
  # it were a subproject within our project.
  target_include_directories(rocksdb
    PUBLIC $<BUILD_INTERFACE:${rocksdb_SOURCE_DIR}/include>)

  FetchContent_Declare(
    ycsbrunner
    GIT_REPOSITORY git@ssh.dev.azure.com:v3/msr-dsail/LearnedLSM/ycsb-runner
    GIT_TAG        76253d9868d0b3d00196e8922321e3b8bf7b4941
  )
  FetchContent_MakeAvailable(ycsbrunner)
endif()
