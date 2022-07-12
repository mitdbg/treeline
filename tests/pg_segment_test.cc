#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "treeline/slice.h"
#include "page_grouping/key.h"
#include "page_grouping/plr/data.h"
#include "page_grouping/plr/greedy.h"
#include "page_grouping/segment_builder.h"
#include "pg_datasets.h"

using namespace tl;
using namespace tl::pg;

TEST(PLRTest, Sequential) {
  const double delta = 10.0;
  plr::GreedyPLRBuilder64 plr(delta);
  for (const auto& key : Datasets::kSequentialKeys) {
    const auto line = plr.Offer(plr::Point64(key, key));
    ASSERT_FALSE(line.has_value());
  }
  const auto line = plr.Finish();
  ASSERT_TRUE(line.has_value());

  for (const auto& key : Datasets::kSequentialKeys) {
    const double out = line->line()(key);
    ASSERT_LE(std::abs(out - static_cast<double>(key)), delta);
  }
}

TEST(PLRTest, Uniform) {
  const double delta = 10.0;
  plr::GreedyPLRBuilder64 plr(delta);
  std::vector<plr::BoundedLine64> lines;
  for (size_t i = 0; i < Datasets::kUniformKeys.size(); ++i) {
    const auto line = plr.Offer(plr::Point64(Datasets::kUniformKeys[i], i));
    if (line.has_value()) {
      lines.push_back(*line);
    }
  }
  const auto line = plr.Finish();
  if (line.has_value()) {
    lines.push_back(*line);
  }
  ASSERT_FALSE(lines.empty());

  size_t curr_line_idx = 0;
  for (size_t i = 0; i < Datasets::kUniformKeys.size(); ++i) {
    const auto key = Datasets::kUniformKeys[i];
    while (key > lines[curr_line_idx].end()) {
      ++curr_line_idx;
      ASSERT_LT(curr_line_idx, lines.size());
    }
    const double out = lines[curr_line_idx].line()(key);
    ASSERT_LE(std::abs(out - static_cast<double>(i)), delta);
  }
}

void CheckSegments(const std::vector<uint64_t>& dataset,
                   const std::vector<Segment>& segments, const size_t goal,
                   const size_t delta) {
  ASSERT_FALSE(segments.empty());

  for (size_t i = 0; i < segments.size(); ++i) {
    const auto& seg = segments[i];
    if (seg.page_count == 1) {
      ASSERT_FALSE(seg.model.has_value());
    } else {
      ASSERT_TRUE(seg.model.has_value());
    }

    const size_t max_records_in_segment = seg.page_count * (goal + delta);
    const size_t min_records_in_segment = seg.page_count * (goal - delta);
    const size_t num_records = seg.records.size();
    ASSERT_LE(num_records, max_records_in_segment);
    if (i != segments.size() - 1) {
      ASSERT_GE(num_records, min_records_in_segment);
    }

    if (seg.page_count == 1) continue;

    // Check that the keys are distributed into the pages as expected.
    std::vector<size_t> page_counts;
    page_counts.reserve(seg.page_count);
    for (size_t j = 0; j < seg.page_count; ++j) {
      page_counts.push_back(0);
    }

    const Key base_key = seg.records.front().first;
    for (const auto& rec : seg.records) {
      const size_t page_idx =
          PageForKey(base_key, seg.model->line(), seg.page_count, rec.first);
      ++page_counts[page_idx];
    }

    const size_t max_records_in_page = goal + 2 * delta;
    const size_t min_records_in_page = goal - 2 * delta;
    for (size_t j = 0; j < seg.page_count; ++j) {
      ASSERT_LE(page_counts[j], max_records_in_page);
      // The last page in the last segment will not necessarily reach the
      // minimum fill threshold.
      if (!(i == segments.size() - 1 && j == seg.page_count - 1)) {
        ASSERT_GE(page_counts[j], min_records_in_page);
      }
    }
  }

  // Check that all records in the dataset are present in the segments (and are
  // in order).
  size_t dataset_idx = 0;
  for (const auto& seg : segments) {
    for (const auto& rec : seg.records) {
      ASSERT_EQ(dataset[dataset_idx], rec.first);
      ++dataset_idx;
    }
  }
}

TEST(SegmentBuilderTest, Sequential_44_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(Datasets::kSequentialKeys.size());
  for (const auto& key : Datasets::kSequentialKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 44;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.BuildFromDataset(records);

  CheckSegments(Datasets::kSequentialKeys, segments, goal, delta);
}

TEST(SegmentBuilderTest, Sequential_15_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(Datasets::kSequentialKeys.size());
  for (const auto& key : Datasets::kSequentialKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 15;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.BuildFromDataset(records);

  CheckSegments(Datasets::kSequentialKeys, segments, goal, delta);
}

TEST(SegmentBuilderTest, Uniform_44_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(Datasets::kUniformKeys.size());
  for (const auto& key : Datasets::kUniformKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 44;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.BuildFromDataset(records);

  CheckSegments(Datasets::kUniformKeys, segments, goal, delta);
}

TEST(SegmentBuilderTest, Uniform_15_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(Datasets::kUniformKeys.size());
  for (const auto& key : Datasets::kUniformKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 15;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.BuildFromDataset(records);

  CheckSegments(Datasets::kUniformKeys, segments, goal, delta);
}
