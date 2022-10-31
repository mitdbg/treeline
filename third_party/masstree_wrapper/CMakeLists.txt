set(masstree_wrapper_sources
  masstree_wrapper.cc
  masstree_wrapper.h 
)

target_sources(treeline PRIVATE ${masstree_wrapper_sources})
target_sources(pg_treeline PRIVATE ${masstree_wrapper_sources})
