# Synthetic Write: Measure the write throughput when writing synthetic data.
# This benchmark compares the Learned LSM database against RocksDB.
add_executable(synth_write)
target_sources(synth_write
  PRIVATE
    data.cc
    data.h
    guard.h
    timing.h
    synth_write.cc)
target_include_directories(synth_write PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(synth_write gflags llsm rocksdb)
