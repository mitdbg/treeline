#include "file_address.h"

namespace llsm {

// Define static variables (default to 1-segment case).
size_t FileAddress::segment_bits_ = 0;
size_t FileAddress::offset_bits_ = (sizeof(size_t) << 3);

}  // namespace llsm