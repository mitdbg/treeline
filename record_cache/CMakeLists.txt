set(record_cache_sources
  record_cache_entry.cc
  record_cache_entry.h
  record_cache.cc
  record_cache.h
)

target_sources(llsm PRIVATE ${record_cache_sources})
target_sources(pg_llsm PRIVATE ${record_cache_sources})
