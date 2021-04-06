#pragma once

#include <cstdlib>
#include <iostream>

namespace llsm {

// Manages the representation of the logical page id of the page. This is the
// page id output by the model and doesn't need to be correlated with location
// on disk.
class LogicalPageId {
  // The mask used to extract the bit indicating an overflow page.
 private:
  static constexpr size_t kOverflowBit = 1ULL << 63;

 public:
  LogicalPageId(const size_t raw_page_id = 0, const bool is_overflow = false) {
    value_ = raw_page_id;
    if (is_overflow) MarkAsOverflow();
  }

  void MarkAsOverflow() { value_ |= kOverflowBit; }

  bool IsOverflow() const { return value_ & kOverflowBit; }

  size_t GetRawPageId() const { return value_ & ~kOverflowBit; }

  // Comparison operators
  inline bool operator==(const LogicalPageId& rhs) const {
    return this->value_ == rhs.value_;
  }
  inline bool operator!=(const LogicalPageId& rhs) const {
    return !(*this == rhs);
  }
  inline bool operator<(const LogicalPageId& rhs) {
    return this->value_ < rhs.value_;
  }
  inline bool operator>(const LogicalPageId& rhs) {
    return this->value_ > rhs.value_;
  }
  inline bool operator<=(const LogicalPageId& rhs) { return !(*this > rhs); }
  inline bool operator>=(const LogicalPageId& rhs) { return !(*this < rhs); }
  inline LogicalPageId operator++() {
    LogicalPageId old = *this;
    value_++;
    return old;
  }

 private:
  size_t value_;
};

}  // namespace llsm

namespace std {

ostream& operator<<(ostream& os, const llsm::LogicalPageId& id);

template <>
struct hash<llsm::LogicalPageId> {
  std::size_t operator()(const llsm::LogicalPageId& id) const {
    return std::hash<size_t>()(id.GetRawPageId());
  }
};

}  // namespace std