find_library(
  libjemalloc
  jemalloc
  HINTS
    $ENV{HOME}/lib
    $ENV{HOME}/opt/lib
  REQUIRED)

set(masstree_sources
  compiler.cc
	str.cc
	string.cc
	straccum.cc
	json.cc)

set(common_optimization_options
  -funroll-loops
  -fno-omit-frame-pointer)

# Configure masstree to generate config.h
get_filename_component(libjemalloc_dir ${libjemalloc} DIRECTORY)
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/config.h
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && 
    ${CMAKE_CURRENT_SOURCE_DIR}/bootstrap.sh &&
    ${CMAKE_CURRENT_SOURCE_DIR}/configure
      --with-malloc=jemalloc
      --disable-assertions
      --disable-invariants
      --disable-preconditions
      --enable-max-key-len=1024
      LDFLAGS=-L${libjemalloc_dir}
  VERBATIM)


add_library(masstree)
target_sources(masstree PRIVATE ${masstree_sources} ${CMAKE_CURRENT_SOURCE_DIR}/config.h)
target_compile_options(masstree
  PRIVATE
    -include ${CMAKE_CURRENT_SOURCE_DIR}/config.h
    ${common_optimization_options})
