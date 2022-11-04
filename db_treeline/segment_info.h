#pragma once

#include <optional>

#include "key.h"
#include "persist/segment_id.h"
#include "plr/data.h"

namespace tl {
namespace pg {

class SegmentInfo {
 public:
  SegmentInfo(SegmentId id, std::optional<plr::Line64> model)
      : raw_id_(id.value() & kSegmentIdMask), model_(model) {}

  // Represents an invalid `SegmentInfo`.
  SegmentInfo() : raw_id_(kInvalidSegmentId) {}

  SegmentId id() const {
    return raw_id_ == kInvalidSegmentId ? SegmentId()
                                        : SegmentId(raw_id_ & kSegmentIdMask);
  }

  size_t page_count() const { return 1ULL << id().GetFileId(); }

  const std::optional<plr::Line64>& model() const { return model_; }

  size_t PageForKey(Key base_key, Key candidate) const {
    const size_t pages = page_count();
    if (pages == 1) return 0;
    return pg::PageForKey(base_key, *model_, pages, candidate);
  }

  bool operator==(const SegmentInfo& other) const {
    return raw_id_ == other.raw_id_ && model_ == other.model_;
  }

  void SetOverflow(bool overflow) {
    if (overflow) {
      raw_id_ |= kHasOverflowMask;
    } else {
      raw_id_ &= kSegmentIdMask;
    }
  }

  bool HasOverflow() const { return (raw_id_ & kHasOverflowMask) != 0; }

 private:
  // Most significant bit used to indicate whether or not this segment has an
  // overflow. The remaining 63 bits hold the `SegmentId` value. If all 63 bits
  // are set to 1, we assume the ID is invalid.
  size_t raw_id_;
  std::optional<plr::Line64> model_;

  static constexpr size_t kHasOverflowMask = (1ULL << 63);
  static constexpr size_t kSegmentIdMask = ~(kHasOverflowMask);
  static constexpr size_t kInvalidSegmentId = kSegmentIdMask;
};

}  // namespace pg
}  // namespace tl
