add_executable(sanity_test sanity.cc)
target_link_libraries(sanity_test PRIVATE gtest gtest_main llsm)
