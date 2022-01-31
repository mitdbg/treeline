#pragma once

#include "page.h"
#include <cstdint>

namespace llsm {
namespace pg {

// A thin wrapper around a group of pages (a segment) to provide utility methods
// for the segment as a whole.
class SegmentWrap {
 public:
  SegmentWrap(void* data, const size_t pages_in_segment);

  uint32_t GetSequenceNumber() const;
  void SetSequenceNumber(uint32_t sequence);

  bool CheckChecksum() const;
  void ComputeAndSetChecksum();

 private:
  Page PageAtIndex(size_t index) const;
  uint32_t ComputeChecksum() const;

  void* data_;
  size_t pages_in_segment_;
};

}  // namespace pg
}  // namespace llsm
