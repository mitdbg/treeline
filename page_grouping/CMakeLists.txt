# Pseudo target used to simplify compiling all page grouping related targets.
add_custom_target(pg_all)
add_dependencies(pg_all pg_treeline)

# The page grouping sources.
add_library(pg STATIC)
target_sources(pg PRIVATE
  persist/page.cc
  persist/page.h
  persist/segment_id.cc
  persist/segment_id.h
  persist/segment_wrap.cc
  persist/segment_wrap.h
  plr/data.h
  plr/greedy.h
  circular_page_buffer.h
  free_list.cc
  free_list.h
  key.cc
  key.h
  lock_manager.cc
  lock_manager.h
  manager_load.cc
  manager_rewrite.cc
  manager_scan_prefetch.cc
  manager_scan.cc
  manager.cc
  manager.h
  pg_stats.cc
  rand_exp_backoff.cc
  rand_exp_backoff.h
  segment_builder.cc
  segment_builder.h
  segment_index.cc
  segment_index.h
  workspace.h
  ../bufmgr/page_memory_allocator.cc
  ../bufmgr/page_memory_allocator.h
  ../third_party/tlx/btree_map.h
  ../third_party/tlx/btree.h
  ../third_party/tlx/core.cc
  ../third_party/tlx/core.h
  ../util/status.cc
  ../util/thread_pool.cc
)
target_include_directories(pg
  PUBLIC
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(pg PUBLIC libcuckoo PRIVATE crc32c)
add_dependencies(pg_all pg)

target_sources(pg_treeline PRIVATE
  pg_db_impl.cc
  pg_db_impl.h
)
target_link_libraries(pg_treeline PRIVATE pg)

# The original TreeLine source also uses the record cache, which depends on
# definitions in `pg_stats.cc`.
target_sources(treeline PRIVATE pg_stats.cc)

if(TL_BUILD_BENCHMARKS)
  # Primary workload runner.
  add_executable(pg_bench
    config.cc
    config.h
    pg_bench.cc
    pg_interface.h
    ../bench/common/load_data.cc
    ../bench/common/load_data.h
    ../bench/common/startup.cc
    ../bench/common/startup.h
  )
  target_link_libraries(pg_bench PRIVATE pg gflags ycsbr-gen Threads::Threads)

  # Used to shuffle pages on disk.
  add_executable(pg_shuffle tools/shuffle_pages.cc)
  target_link_libraries(pg_shuffle PRIVATE gflags)

  add_executable(pg_flatten tools/flatten.cc)
  target_link_libraries(pg_flatten PRIVATE pg_treeline gflags)

  # Contains debug tools.
  add_subdirectory(debug)

  add_dependencies(pg_all
    pg_bench
    pg_check
    pg_read
    pg_read2
    pg_shuffle
    pg_standalone
    pg_flatten
  )
endif()
