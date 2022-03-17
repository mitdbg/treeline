set(masstree_wrapper_sources
  masstree_wrapper.cc
  masstree_wrapper.h 
)

target_sources(llsm PRIVATE ${masstree_wrapper_sources})
target_sources(pg_llsm PRIVATE ${masstree_wrapper_sources})
