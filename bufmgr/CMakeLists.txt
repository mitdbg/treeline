target_sources(treeline PRIVATE
  buffer_frame.cc
  buffer_frame.h
  buffer_manager.cc
  buffer_manager.h
  file_manager.cc
  file_manager.h
  file.h
  lru_eviction.cc
  lru_eviction.h
  options.h
  page_eviction_strategy.h
  page_memory_allocator.cc
  page_memory_allocator.h
  physical_page_id.cc
  physical_page_id.h
  twoqueue_eviction.cc
  twoqueue_eviction.h)
