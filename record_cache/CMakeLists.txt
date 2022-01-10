target_sources(llsm PRIVATE
  record_cache_entry.cc
  record_cache_entry.h
  )

find_package (Threads)

add_library(ARTSynchronized ../art_olc/Tree.cpp)
target_link_libraries(ARTSynchronized tbb Threads::Threads)
add_executable(min_test min_test.cc record_cache_entry.cc)
target_link_libraries(min_test ARTSynchronized)
