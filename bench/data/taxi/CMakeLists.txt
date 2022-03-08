cmake_minimum_required(VERSION 3.17)
project(data LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -fsanitize=address")

# S2Geometry
include(ExternalProject)
include(FetchContent)
FetchContent_Declare(
  s2
  GIT_REPOSITORY https://github.com/google/s2geometry.git
  GIT_TAG        v0.9.0
)
FetchContent_MakeAvailable(s2)

add_executable(contaxi contaxi.cc ../common.h)
target_link_libraries(contaxi s2)
