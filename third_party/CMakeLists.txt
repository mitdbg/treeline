include(ExternalProject)
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

# Concurrent hash table
FetchContent_Declare(
  libcuckoo
  GIT_REPOSITORY https://github.com/efficient/libcuckoo.git
  GIT_TAG        784d0f5d147b9a73f897ae55f6c3712d9a91b058
)
FetchContent_MakeAvailable(libcuckoo)

if(TL_BUILD_TESTS)
  # Google Tests: Fetch it iff we need to build the test suite
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        release-1.11.0
  )
  FetchContent_MakeAvailable(googletest)
endif()

if(TL_BUILD_BENCHMARKS)
  FetchContent_Declare(
    gflags
    GIT_REPOSITORY https://github.com/gflags/gflags
    GIT_TAG        v2.2.2
  )
  FetchContent_MakeAvailable(gflags)

  FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark
    GIT_TAG        v1.6.1
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
  set(FAIL_ON_WARNINGS OFF)
  FetchContent_MakeAvailable(rocksdb)

  # The RocksDB CMakeLists.txt exports RocksDB as a static library called
  # "rocksdb". However they do not specify the path to the headers properly.
  # This target_include_directories() call below lets us link to RocksDB as if
  # it were a subproject within our project.
  target_include_directories(rocksdb
    PUBLIC $<BUILD_INTERFACE:${rocksdb_SOURCE_DIR}/include>)

  FetchContent_Declare(
    ycsbr
    GIT_REPOSITORY https://github.com/mitdbg/ycsbr.git
    GIT_TAG        v4.1.0
  )
  FetchContent_MakeAvailable(ycsbr)

  # This sets up libaio as a local dependency, which is used by LeanStore. We
  # use `aio` as the target name because LeanStore links against it. We only run
  # this setup if the `aio` target does not already exist.
  #
  # We use both FetchContent and ExternalProject to set up this dependency.
  # FetchContent allows us to download the libaio library when we configure the
  # project. ExternalProject helps us compile the library's sources when make is
  # used to build the entire project.
  if (NOT TARGET aio)
    message(STATUS "Building libaio v0.3.112 from source because it was not found locally.")
    FetchContent_Declare(
      libaio_src
      GIT_REPOSITORY https://pagure.io/libaio.git
      GIT_TAG        libaio-0.3.112
    )
    FetchContent_GetProperties(libaio_src)
    if (NOT libaio_src_POPULATED)
      FetchContent_Populate(libaio_src)
    endif()
    ExternalProject_Add(
      libaio_src
      SOURCE_DIR ${libaio_src_SOURCE_DIR}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND "make"
      BUILD_IN_SOURCE ON
      INSTALL_COMMAND ""
    )
    add_library(aio STATIC IMPORTED GLOBAL)
    add_dependencies(aio libaio_src)
    set_target_properties(
      aio PROPERTIES IMPORTED_LOCATION
        ${libaio_src_SOURCE_DIR}/src/libaio.a)
    target_include_directories(aio INTERFACE
      ${libaio_src_SOURCE_DIR}/src)
  endif()

  set(BUILD_LEANSTORE_FRONTEND OFF)
  add_subdirectory(leanstore)
endif()

# Include embedded CMake projects.
add_subdirectory(tlx)
add_subdirectory(masstree)
add_subdirectory(masstree_wrapper)
