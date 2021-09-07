target_sources(llsm PRIVATE
  db_impl_range.cc
  db_impl_reorg.cc
  db_impl.cc
  db_impl.h
  format.h
  logger.cc
  logger.h
  manifest.cc
  manifest.h
  memtable.cc
  memtable.h
  options.cc
  options.h
  page.cc
  page.h
  posix_logger.h
  slice.cc)
