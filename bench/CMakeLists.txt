# Common utilities used in the LLSM benchmarks.
add_library(bench_common
  common/data.cc
  common/data.h
  common/timing.h)
# Any targets linking to `bench_common` will also link to `llsm` and will be
# able to include LLSM internal headers.
target_link_libraries(bench_common PUBLIC llsm)
target_include_directories(bench_common PUBLIC ${PROJECT_SOURCE_DIR})

# All `bench_common` utilities plus configuration utilities used in benchmarks
# comparing LLSM against RocksDB.
#
# We use this additional library target because not all benchmark executables
# need to link to RocksDB and gflags (e.g., `microbench`).
add_library(bench_common_config common/config.cc common/config.h)
target_link_libraries(bench_common_config PUBLIC bench_common gflags rocksdb)

# Synthetic Write: Measure the write throughput when writing synthetic data.
# This benchmark compares the Learned LSM database against RocksDB.
add_executable(synth_write synth_write.cc)
target_link_libraries(synth_write bench_common_config)

# YCSB: Run extracted YCSB workloads against LLSM and RocksDB.
add_executable(ycsb ycsb.cc)
target_link_libraries(ycsb bench_common_config ycsbrunner)

# Microbench: An executable containing all of LLSM's microbenchmarks.
add_executable(microbench
  memtable_benchmark.cc
  packed_map_benchmark.cc
  thread_pool_benchmark.cc)
target_link_libraries(microbench
  bench_common
  benchmark::benchmark
  benchmark::benchmark_main)

# Deferred I/O: A test microbenchmark for deferring certain I/O operations.
add_executable(deferred_io deferred_io_simulation.cc)
target_link_libraries(deferred_io bench_common gflags ycsbrunner)

