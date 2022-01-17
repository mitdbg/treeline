add_library(pg STATIC)
target_sources(pg PRIVATE
  persist/segment_id.cc
  persist/segment_id.h
  plr/data.h
  plr/greedy.h
  manager.cc
  manager.h
  ../tlx/btree_map.h
  ../tlx/btree.h
  ../tlx/core.cc
  ../tlx/core.h
)
target_include_directories(pg
  PUBLIC
    ${PROJECT_SOURCE_DIR}
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(pg_bench
  pg_bench.cc
  pg_interface.h
  ../bench/common/load_data.cc
  ../bench/common/load_data.h
  ../bench/common/startup.cc
  ../bench/common/startup.h
)
target_link_libraries(pg_bench PRIVATE pg gflags ycsbr-gen Threads::Threads)
