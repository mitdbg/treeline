#include "segment_id.h"

namespace tl {
namespace pg {

// Most significant 4 bits reserved for the file (only 3 are actually used for
// the file ID; the most significant bit is reserved).
size_t SegmentId::offset_bits_ = 60;
size_t SegmentId::file_mask_ = 15ULL << SegmentId::offset_bits_;
size_t SegmentId::offset_mask_ = ~(SegmentId::file_mask_);

}  // namespace pg
}  // namespace tl

namespace std {

ostream& operator<<(ostream& os, const tl::pg::SegmentId& id) {
  os << id.GetFileId() << "-" << id.GetOffset();
  return os;
}

}  // namespace std
