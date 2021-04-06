#include "physical_page_id.h"

namespace llsm {

// Define static variables (default to 1-segment case).
size_t PhysicalPageId::segment_bits_ = 0;
size_t PhysicalPageId::offset_bits_ = (sizeof(size_t) << 3);

}  // namespace llsm