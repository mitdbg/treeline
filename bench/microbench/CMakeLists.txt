# Synthetic Write: Measure the write throughput when writing synthetic data.
# This benchmark compares TreeLine against RocksDB.
add_executable(synth_write synth_write.cc)
target_link_libraries(synth_write bench_common_config)

# Microbench: An executable containing Google benchmark-based microbenchmarks.
add_executable(microbench
  memtable_benchmark.cc
  packed_map_benchmark.cc
  thread_pool_benchmark.cc)
target_link_libraries(microbench
  bench_common
  benchmark::benchmark
  benchmark::benchmark_main)

# Hash Table YCSB: Run extracted YCSB workloads against different hash table implementations.
add_executable(hashtable_ycsb hashtable_ycsb.cc)
target_link_libraries(hashtable_ycsb
  PRIVATE
    Threads::Threads
    gflags
    ycsbr
    bench_common
    libcuckoo
)

# Record cache: A microbenchmark for the performance of the ART-based record cache.
add_executable(record_cache record_cache_benchmark.cc)
target_link_libraries(record_cache 
  bench_common
  benchmark::benchmark
  benchmark::benchmark_main)

# Deferred I/O: A test microbenchmark for deferring certain I/O operations.
add_executable(deferred_io deferred_io_simulation.cc)
target_link_libraries(deferred_io bench_common gflags ycsbr)

# Write-ahead Log: A microbenchmark used to evaluate the overhead of writing to
# a write-ahead log.
add_executable(wal wal_benchmark.cc)
target_link_libraries(wal bench_common gflags benchmark::benchmark)

# Buffer manager simulation: A microbenchmark for the added latency of the 
# buffer manager, excluding the underlying I/O operations.
add_executable(bufmgr_benchmark buffer_manager_benchmark.cc)
target_link_libraries(bufmgr_benchmark bench_common_config gflags ycsbr benchmark::benchmark)
