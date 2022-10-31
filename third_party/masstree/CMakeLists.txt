find_library(
  libjemalloc
  jemalloc
  HINTS
    $ENV{HOME}/lib
    $ENV{HOME}/opt/lib
  REQUIRED)

set(masstree_sources
  checkpoint.cc
  clp.c
  compiler.cc
  file.cc
  json.cc
  kvio.cc
  kvrandom.cc
  kvthread.cc
  log.cc
  memdebug.cc
  misc.cc
  msgpack.cc
  perfstat.cc
  query_masstree.cc
	str.cc
  straccum.cc
  string_slice.cc
	string.cc
  value_array.cc
  value_string.cc
  value_versioned_array.cc
	)

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

add_library(masstree STATIC)
set_source_files_properties(clp.c PROPERTIES LANGUAGE CXX )
target_sources(masstree PRIVATE ${masstree_sources} ${CMAKE_CURRENT_SOURCE_DIR}/config.h)
target_compile_options(masstree
  PRIVATE
    -include ${CMAKE_CURRENT_SOURCE_DIR}/config.h
    ${common_optimization_options})
