# Pseudo target used to simplify compiling all page grouping related targets.
add_custom_target(pg_all)

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
  key.h
  manager_load.cc
  manager_rewrite.cc
  manager_scan.cc
  manager.cc
  manager.h
  segment_builder.cc
  segment_builder.h
  workspace.h
  ../bufmgr/page_memory_allocator.cc
  ../bufmgr/page_memory_allocator.h
  ../tlx/btree_map.h
  ../tlx/btree.h
  ../tlx/core.cc
  ../tlx/core.h
  ../util/status.cc
  ../util/thread_pool.cc
)
target_include_directories(pg
  PUBLIC
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(pg PRIVATE crc32c)
add_dependencies(pg_all pg)

if(LLSM_BUILD_BENCHMARKS)
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

  # Contains debug tools.
  add_subdirectory(debug)

  add_dependencies(pg_all
    pg_bench
    pg_check
    pg_read
    pg_read2
    pg_scan
    pg_shuffle
    pg_standalone
  )
endif()

if(LLSM_BUILD_TESTS)
  # Page grouping tests (kept separate from the rest of the LLSM tests).
  add_executable(pg_test
    tests/datasets.cc
    tests/datasets.h
    tests/manager_rewrite_test.cc
    tests/manager_test.cc
    tests/segment_test.cc
  )
  target_link_libraries(pg_test PRIVATE pg gtest gtest_main)
  add_dependencies(pg_all pg_test)
endif()
