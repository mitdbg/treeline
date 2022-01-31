add_library(pg STATIC)
target_sources(pg PRIVATE
  persist/page.cc
  persist/page.h
  persist/segment_id.cc
  persist/segment_id.h
  plr/data.h
  plr/greedy.h
  key.h
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
)
target_include_directories(pg
  PUBLIC
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/include
)

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

add_executable(pg_test
  tests/datasets.cc
  tests/datasets.h
  tests/manager_test.cc
  tests/segment_test.cc
)
target_link_libraries(pg_test PRIVATE pg gtest gtest_main)

add_executable(pg_shuffle tools/shuffle_pages.cc)
target_link_libraries(pg_shuffle PRIVATE gflags)

add_subdirectory(debug)

if(NOT TARGET pg_all)
  add_custom_target(pg_all)
  add_dependencies(pg_all
    pg_bench
    pg_test
    pg_shuffle
    pg_read
    pg_read2
    pg_standalone
  )
endif()
