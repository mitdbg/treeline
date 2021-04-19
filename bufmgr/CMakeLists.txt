target_sources(llsm PRIVATE
  buffer_frame.cc
  buffer_frame.h
  buffer_manager.cc
  buffer_manager.h
  file_manager.cc
  file_manager.h
  file.h
  physical_page_id.cc
  physical_page_id.h
  hash_queue.h
  options.h
  page_eviction_strategy.h
  twoqueue_eviction.cc
  twoqueue_eviction.h)
