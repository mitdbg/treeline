# This file contains targets used for page grouping debugging.

add_executable(pg_standalone
  pg_standalone.cc
  ../../bench/common/load_data.cc
  ../../bench/common/load_data.h
)
target_link_libraries(pg_standalone PRIVATE pg gflags)

add_executable(pg_read pg_read.cc)
target_link_libraries(pg_read PRIVATE gflags)

add_executable(pg_read2 pg_read2.cc)
target_link_libraries(pg_read2 PRIVATE gflags)

add_executable(pg_check pg_check.cc)
target_link_libraries(pg_check PRIVATE pg gflags Threads::Threads)
