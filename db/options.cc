#include "treeline/options.h"

#include <cassert>

#include "db/page.h"

namespace tl {

size_t KeyDistHints::records_per_page() const {
  assert(page_fill_pct > 0 && page_fill_pct <= 100);
  const double fill_pct = page_fill_pct / 100.0;
  return fill_pct * Page::NumRecordsThatFit(record_size, 2 * key_size);
}

size_t KeyDistHints::num_pages() const {
  if (num_keys == 0) return 1;
  size_t rec_per_page = records_per_page();
  return (num_keys / rec_per_page) + ((num_keys % rec_per_page) != 0);
}

}  // namespace tl
