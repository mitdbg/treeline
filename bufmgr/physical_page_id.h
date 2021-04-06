#pragma once

#include <cstdlib>

#include "util/calc.h"

namespace llsm {

// Stores the file and PAGE-offset within the file where a specific page can
// be found.
//
// Calling `SetBitWidths()` may make any PhysicalPageId objects already created
// invalid, so it should only be called exactly once, before creating any
// objects.
class PhysicalPageId {
 public:
  static void SetBitWidths(size_t total_segments) {
    segment_bits_ = Pow2Ceil(total_segments);
    offset_bits_ = (sizeof(size_t) << 3) - segment_bits_;
  }

  PhysicalPageId(const size_t file_id, const size_t offset) {
    Update(file_id, offset);
  }

  void Update(const size_t file_id, const size_t offset) {
    value_ = (file_id << offset_bits_) + offset;
  }

  size_t GetFileId() const { return value_ >> offset_bits_; }

  size_t GetOffset() const {
    return (value_ << segment_bits_) >> segment_bits_;
  }

 private:
  // The number of bits of `value_` allocated to each component of a
  // PhysicalPageId.
  static size_t segment_bits_;
  static size_t offset_bits_;

  // The highest-order `segment_bits_` store the file_id, while the rest store
  // the page offset.
  size_t value_ = 0;
};

}  // namespace llsm