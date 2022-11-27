# Common utilities used in the TreeLine benchmarks.
add_library(bench_common
  common/data.cc
  common/data.h
  common/load_data.cc
  common/load_data.h
  common/startup.cc
  common/startup.h
  common/timing.h)
# Any targets linking to `bench_common` will also link to `treeline` and will be
# able to include TreeLine internal headers.
target_link_libraries(bench_common PUBLIC treeline)
target_include_directories(bench_common
  PUBLIC
    ${PROJECT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# All `bench_common` utilities plus configuration utilities used in benchmarks
# comparing TreeLine against RocksDB.
#
# We use this additional library target because not all benchmark executables
# need to link to RocksDB and gflags (e.g., `microbench`).
add_library(bench_common_config
  common/config.cc
  common/config.h
  common/kvell_interface.h
  common/leanstore_interface.h
  common/treeline_interface.h
  common/pg_treeline_interface.h
  common/rocksdb_interface.h)
target_link_libraries(bench_common_config PUBLIC
  bench_common
  gflags
  rocksdb
  leanstore
  pg_treeline
  libcuckoo)

# The microbench directory contains microbenchmarks used to guide TreeLine's
# development.
add_subdirectory(microbench)

# Run Custom: An executable that runs YSCBR-generated workloads against TreeLine
# and RocksDB.
add_executable(run_custom run_custom.cc)
target_link_libraries(run_custom bench_common_config ycsbr-gen)
