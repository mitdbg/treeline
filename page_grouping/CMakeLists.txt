add_executable(pg_bench
  pg_bench.cc
  pg_interface.h
  ../bench/common/load_data.cc
  ../bench/common/load_data.h
  ../bench/common/startup.cc
  ../bench/common/startup.h
)
target_link_libraries(pg_bench PRIVATE gflags ycsbr-gen Threads::Threads)
target_include_directories(pg_bench PUBLIC ${PROJECT_SOURCE_DIR})
