add_executable(tester)
target_sources(tester
  PRIVATE
    packed_map_test.cc
    sanity_test.cc)
target_link_libraries(tester llsm gtest gtest_main Threads::Threads)
# Allow tests to also include LLSM internal headers
target_include_directories(tester PRIVATE ${PROJECT_SOURCE_DIR})
