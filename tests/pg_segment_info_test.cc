#include <optional>
#include "gtest/gtest.h"
#include "page_grouping/persist/segment_id.h"
#include "page_grouping/plr/data.h"
#include "page_grouping/segment_info.h"

namespace {

using namespace tl;
using namespace tl::pg;

TEST(SegmentInfoTest, Invalid) {
  SegmentInfo invalid;
  ASSERT_FALSE(invalid.id().IsValid());
}

TEST(SegmentInfoTest, NoOverflowDefault) {
  SegmentId id(0, 16081);
  SegmentInfo info(id, std::optional<plr::Line64>());
  ASSERT_FALSE(info.HasOverflow());
}

TEST(SegmentInfoTest, NoOverflowDefault2) {
  SegmentId id(0, 125);
  SegmentInfo info(id, plr::Line64(1.0, 1.0));
  ASSERT_FALSE(info.HasOverflow());
}

TEST(SegmentInfoTest, SetOverflow) {
  SegmentId id(0, 100);
  SegmentInfo info(id, plr::Line64(1.0, 1.0));
  ASSERT_FALSE(info.HasOverflow());
  info.SetOverflow(true);
  ASSERT_TRUE(info.HasOverflow());
}

}  // namespace
