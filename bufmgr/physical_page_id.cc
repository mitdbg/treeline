#include "physical_page_id.h"

namespace tl {

// Define static variables (default to 1-segment case).
size_t PhysicalPageId::offset_bits_ = (sizeof(size_t) << 3);

size_t PhysicalPageId::segment_mask_ = 0ULL;
size_t PhysicalPageId::offset_mask_ = ~(0ULL);

}  // namespace tl

namespace std {

ostream& operator<<(ostream& os, const tl::PhysicalPageId& id) {
  os << id.GetFileId() << "-" << id.GetOffset();
  return os;
}

}  // namespace std