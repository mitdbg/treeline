#include "segment_id.h"

namespace llsm {
  namespace pg {

// Most significant 3 bits reserved for the file.
size_t SegmentId::offset_bits_ = 61;
size_t SegmentId::file_mask_ = 7ULL << SegmentId::offset_bits_;
size_t SegmentId::offset_mask_ = ~(SegmentId::file_mask_);

}
}  // namespace llsm

namespace std {

ostream& operator<<(ostream& os, const llsm::pg::SegmentId& id) {
  os << id.GetFileId() << "-" << id.GetOffset();
  return os;
}

}  // namespace std
