set(record_cache_sources
  record_cache_entry.cc
  record_cache_entry.h
  record_cache.cc
  record_cache.h
)

target_sources(tl PRIVATE ${record_cache_sources})
target_sources(pg_tl PRIVATE ${record_cache_sources})
