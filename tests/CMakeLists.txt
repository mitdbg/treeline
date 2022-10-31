include(GoogleTest)

add_executable(tester)
target_sources(tester
  PRIVATE
    buffer_manager_test.cc
    coding_test.cc
    db_test.cc
    file_manager_test.cc
    insert_tracker_test.cc
    manifest_test.cc
    memtable_test.cc
    packed_map_test.cc
    page_test.cc
    pg_datasets.cc
    pg_datasets.h
    pg_db_test.cc
    pg_lock_manager_test.cc
    pg_manager_rewrite_test.cc
    pg_manager_test.cc
    pg_segment_info_test.cc
    pg_segment_test.cc
    record_cache_test.cc
    thread_pool_test.cc
    wal_manager_test.cc
    wal_rw_test.cc
)
target_link_libraries(tester
  PRIVATE
    treeline
    pg_treeline
    pg
    gtest
    gtest_main
    Threads::Threads
    crc32c
)
# Allow tests to also include TreeLine internal headers
target_include_directories(tester PRIVATE ${PROJECT_SOURCE_DIR})

# Register our tests with CTest
gtest_add_tests(TARGET tester)
