# Google Tests: Fetch it iff we need to build the test suite
if(LLSM_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        release-1.10.0
  )
  FetchContent_MakeAvailable(googletest)
endif()
