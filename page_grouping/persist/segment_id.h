#pragma once

#include <cstdlib>
#include <iostream>
#include <stdexcept>

#include "util/calc.h"

namespace tl {
namespace pg {

// Stores the file and PAGE-offset within the file where a specific segment can
// be found. This class is adapted from `tl::PhysicalPageId`.
class SegmentId {
 public:
  SegmentId() : value_(kInvalidValue) {}

  // Used in deserialization.
  explicit SegmentId(size_t value) : value_(value) {}

  SegmentId(const size_t file_id, const size_t offset) {
    CheckFileId(file_id);
    CheckOffset(offset);
    const size_t temp_value =
        (offset & offset_mask_) | ((file_id << offset_bits_) & file_mask_);
    CheckValid(temp_value);
    value_ = temp_value;
  }

  void SetOffset(const size_t offset) {
    CheckOffset(offset);
    size_t temp_value = (value_ & ~offset_mask_) | (offset & offset_mask_);
    CheckValid(temp_value);
    value_ = temp_value;
  }

  void SetFileId(const size_t file_id) {
    CheckFileId(file_id);
    size_t temp_value =
        (value_ & ~file_mask_) | ((file_id << offset_bits_) & file_mask_);
    CheckValid(temp_value);
    value_ = temp_value;
  }

  size_t GetFileId() const { return value_ >> offset_bits_; }

  size_t GetOffset() const { return value_ & offset_mask_; }

  bool IsValid() const { return value_ != kInvalidValue; }

  size_t value() const { return value_; }

  // Comparison operators
  inline bool operator==(const SegmentId& rhs) const {
    return this->value_ == rhs.value_;
  }
  inline bool operator!=(const SegmentId& rhs) const {
    return !(*this == rhs);
  }

 private:
  // A special value used to indicate an "invalid" id.
  static const size_t kInvalidValue = ~(0ULL);

  static void CheckValid(const size_t temp_value) {
    if (temp_value == kInvalidValue) {
      throw std::runtime_error(
          "Failed to create PageId: tried to use reserved value.");
    }
  }

  static void CheckFileId(const size_t file_id) {
    if ((file_id & ~(file_mask_ >> offset_bits_)) != 0) {
      throw std::runtime_error("Failed to create PageId: file_id overflow.");
    }
  }

  static void CheckOffset(const size_t offset) {
    if ((offset & ~offset_mask_) != 0) {
      throw std::runtime_error("Failed to create PageId: offset overflow.");
    }
  }

  // The number of bits of `value_` allocated to each component of a
  // `SegmentId`.
  static size_t offset_bits_;
  static size_t file_mask_;
  static size_t offset_mask_;

  // The lowest-order `offset_bits_` store the page offset, while the rest store
  // the file_id.
  size_t value_;

  friend struct std::hash<tl::pg::SegmentId>;
};

}  // namespace pg
}  // namespace tl

namespace std {

ostream& operator<<(ostream& os, const tl::pg::SegmentId& id);

template <>
struct hash<tl::pg::SegmentId> {
  std::size_t operator()(const tl::pg::SegmentId& id) const {
    return std::hash<size_t>()(id.value_);
  }
};

}  // namespace std
