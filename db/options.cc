#include "llsm/options.h"

#include <cassert>

#include "db/page.h"

namespace llsm {

size_t KeyDistHints::records_per_page() const {
  assert(page_fill_pct > 0 && page_fill_pct <= 100);
  const double fill_pct = page_fill_pct / 100.0;
  return (Page::UsableSize() - 2 * record_size)  // Need space for fences
         * fill_pct / (record_size + Page::PerRecordMetadataSize());
}

size_t KeyDistHints::num_pages() const {
  if (num_keys == 0) return 1;
  size_t num_pages = num_keys / records_per_page();
  if (num_keys % records_per_page() != 0) ++num_pages;
  return num_pages;
}

}  // namespace llsm
