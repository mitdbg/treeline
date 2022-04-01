# Common utilities used in the LLSM benchmarks.
add_library(bench_common
  common/data.cc
  common/data.h
  common/load_data.cc
  common/load_data.h
  common/startup.cc
  common/startup.h
  common/timing.h)
# Any targets linking to `bench_common` will also link to `tl` and will be
# able to include LLSM internal headers.
target_link_libraries(bench_common PUBLIC tl)
target_include_directories(bench_common PUBLIC ${PROJECT_SOURCE_DIR})

# All `bench_common` utilities plus configuration utilities used in benchmarks
# comparing LLSM against RocksDB.
#
# We use this additional library target because not all benchmark executables
# need to link to RocksDB and gflags (e.g., `microbench`).
add_library(bench_common_config
  common/config.cc
  common/config.h
  common/kvell_interface.h
  common/leanstore_interface.h
  common/tl_interface.h
  common/pg_tl_interface.h
  common/rocksdb_interface.h)
target_link_libraries(bench_common_config PUBLIC bench_common gflags rocksdb leanstore pg_tl libcuckoo)

# Synthetic Write: Measure the write throughput when writing synthetic data.
# This benchmark compares the Learned LSM database against RocksDB.
add_executable(synth_write synth_write.cc)
target_link_libraries(synth_write bench_common_config)

# YCSB: Run extracted YCSB workloads against LLSM and RocksDB.
add_executable(ycsb ycsb.cc)
target_link_libraries(ycsb bench_common_config ycsbr)

# Hash Table YCSB: Run extracted YCSB workloads against different hash table implementations.
add_executable(hashtable_ycsb hashtable_ycsb.cc)
target_link_libraries(hashtable_ycsb Threads::Threads gflags ycsbr)

# Microbench: An executable containing all of LLSM's microbenchmarks.
add_executable(microbench
  memtable_benchmark.cc
  packed_map_benchmark.cc
  thread_pool_benchmark.cc)
target_link_libraries(microbench
  bench_common
  benchmark::benchmark
  benchmark::benchmark_main)

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

# Run Custom: An executable that runs YSCBR-generated workloads against LLSM
# and RocksDB.
add_executable(run_custom run_custom.cc)
target_link_libraries(run_custom bench_common_config ycsbr-gen)
