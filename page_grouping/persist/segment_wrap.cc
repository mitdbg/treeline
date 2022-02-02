#include "segment_wrap.h"

#include <cassert>

#include "../segment_builder.h"
#include "page.h"
#include "util/crc32c.h"

namespace llsm {
namespace pg {

SegmentWrap::SegmentWrap(void* data, const size_t pages_in_segment)
    : data_(data), pages_in_segment_(pages_in_segment) {
  assert(pages_in_segment_ >= SegmentBuilder::kSegmentPageCounts.front());
  assert(pages_in_segment_ <= SegmentBuilder::kSegmentPageCounts.back());
}

uint32_t SegmentWrap::GetSequenceNumber() const {
  if (pages_in_segment_ == 1) {
    return PageAtIndex(0).GetSequenceNumber();
  } else {
    return PageAtIndex(1).GetSequenceNumber();
  }
}

void SegmentWrap::SetSequenceNumber(uint32_t sequence) {
  if (pages_in_segment_ == 1) {
    PageAtIndex(0).SetSequenceNumber(sequence);
  } else {
    PageAtIndex(1).SetSequenceNumber(sequence);
  }
}

bool SegmentWrap::CheckChecksum() const {
  // Single-page segments do not have a checksum (not needed).
  return pages_in_segment_ == 1 ||
         (ComputeChecksum() == PageAtIndex(1).GetChecksum());
}

void SegmentWrap::ComputeAndSetChecksum() {
  // Single-page segments do not have a checksum (not needed).
  if (pages_in_segment_ == 1) return;
  const uint32_t checksum = ComputeChecksum();
  PageAtIndex(1).SetChecksum(checksum);
}

Page SegmentWrap::PageAtIndex(size_t index) const {
  return Page(reinterpret_cast<uint8_t*>(data_) + index * Page::kSize);
}

uint32_t SegmentWrap::ComputeChecksum() const {
  uint32_t checksum = 0;
  for (size_t i = 0; i < pages_in_segment_; ++i) {
    Page page = PageAtIndex(i);
    const auto lower = page.GetLowerBoundary();
    const auto upper = page.GetUpperBoundary();
    checksum = crc32c::Extend(
        checksum, reinterpret_cast<const uint8_t*>(lower.data()), lower.size());
    checksum = crc32c::Extend(
        checksum, reinterpret_cast<const uint8_t*>(upper.data()), upper.size());
  }
  return checksum;
}

void SegmentWrap::ClearAllOverflows() {
  for (size_t i = 0; i < pages_in_segment_; ++i) {
    Page page = PageAtIndex(i);
    page.SetOverflow(SegmentId());
  }
}

bool SegmentWrap::HasOverflow() const {
  for (size_t i = 0; i < pages_in_segment_; ++i) {
    Page page = PageAtIndex(i);
    if (page.HasOverflow()) {
      return true;
    }
  }
  return false;
}

}  // namespace pg
}  // namespace llsm