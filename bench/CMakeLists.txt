# Common utilities used in the LLSM benchmarks.
add_library(bench_common)
target_sources(bench_common
  PRIVATE
    common/data.cc
    common/data.h
    common/guard.h
    common/timing.h)
target_link_libraries(bench_common llsm)
# Any targets linking to `bench_common` can include LLSM internal headers
target_include_directories(bench_common PUBLIC ${PROJECT_SOURCE_DIR})

# Synthetic Write: Measure the write throughput when writing synthetic data.
# This benchmark compares the Learned LSM database against RocksDB.
add_executable(synth_write synth_write.cc)
target_link_libraries(synth_write bench_common gflags llsm rocksdb)

# Microbench: An executable containing all of LLSM's microbenchmarks.
add_executable(microbench
  memtable_benchmark.cc
  thread_pool_benchmark.cc)
target_link_libraries(microbench
  bench_common
  llsm
  benchmark::benchmark
  benchmark::benchmark_main)
