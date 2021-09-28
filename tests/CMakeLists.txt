include(GoogleTest)

add_executable(tester)
target_sources(tester
  PRIVATE
    buffer_manager_test.cc
    coding_test.cc
    file_manager_test.cc
    manifest_test.cc
    memtable_test.cc
    packed_map_test.cc
    page_test.cc
    db_test.cc
    thread_pool_test.cc
    wal_manager_test.cc
    wal_rw_test.cc)
target_link_libraries(tester llsm gtest gtest_main Threads::Threads)
# Allow tests to also include LLSM internal headers
target_include_directories(tester PRIVATE ${PROJECT_SOURCE_DIR})

# Register our tests with CTest
gtest_add_tests(TARGET tester)
