set(masstree_wrapper_sources
  masstree_wrapper.cc
  masstree_wrapper.h 
)

target_sources(tl PRIVATE ${masstree_wrapper_sources})
target_sources(pg_tl PRIVATE ${masstree_wrapper_sources})
