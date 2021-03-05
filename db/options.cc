#include "llsm/options.h"

#include <cassert>

#include "db/page.h"

namespace llsm {

size_t KeyDistHints::records_per_page() const {
  assert(page_fill_pct > 0 && page_fill_pct <= 100);
  const double fill_pct = page_fill_pct / 100.0;
  return Page::kSize * fill_pct / record_size;
}

}  // namespace llsm
