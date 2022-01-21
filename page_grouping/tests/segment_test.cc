#include <algorithm>
#include <cstdint>
#include <random>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../key.h"
#include "../plr/data.h"
#include "../plr/greedy.h"
#include "../segment_builder.h"
#include "gtest/gtest.h"
#include "llsm/slice.h"

using namespace llsm;
using namespace llsm::pg;

// Uniformly selects `num_samples` samples from the range [min_val, max_val]
// without replacement.
std::vector<uint64_t> FloydSample(const size_t num_samples, uint64_t min_val,
                                  uint64_t max_val, std::mt19937& rng) {
  std::unordered_set<uint64_t> samples;
  samples.reserve(num_samples);
  for (uint64_t curr = max_val - num_samples + 1; curr <= max_val; ++curr) {
    std::uniform_int_distribution<uint64_t> dist(min_val, curr);
    const uint64_t next = dist(rng);
    auto res = samples.insert(next);
    if (!res.second) {
      samples.insert(curr);
    }
  }
  assert(samples.size() == num_samples);
  return std::vector<uint64_t>(samples.begin(), samples.end());
}

static const std::vector<uint64_t> kSequentialKeys = ([](const size_t range) {
  std::vector<uint64_t> results;
  results.reserve(range);
  for (uint64_t i = 0; i < range; ++i) {
    results.push_back(i);
  }
  return results;
})(1000);

static const std::vector<uint64_t> kUniformKeys =
    ([](const size_t num_samples, uint64_t min_val, uint64_t max_val) {
      std::mt19937 prng(42);
      auto res = FloydSample(num_samples, min_val, max_val, prng);
      std::sort(res.begin(), res.end());
      return res;
    })(1000, 0, 1000000);

TEST(PLRTest, Sequential) {
  const double delta = 10.0;
  plr::GreedyPLRBuilder64 plr(delta);
  for (const auto& key : kSequentialKeys) {
    const auto line = plr.Offer(plr::Point64(key, key));
    ASSERT_FALSE(line.has_value());
  }
  const auto line = plr.Finish();
  ASSERT_TRUE(line.has_value());

  for (const auto& key : kSequentialKeys) {
    const double out = line->line()(key);
    ASSERT_LE(std::abs(out - static_cast<double>(key)), delta);
  }
}

TEST(PLRTest, Uniform) {
  const double delta = 10.0;
  plr::GreedyPLRBuilder64 plr(delta);
  std::vector<plr::BoundedLine64> lines;
  for (size_t i = 0; i < kUniformKeys.size(); ++i) {
    const auto line = plr.Offer(plr::Point64(kUniformKeys[i], i));
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
  for (size_t i = 0; i < kUniformKeys.size(); ++i) {
    const auto key = kUniformKeys[i];
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
    const size_t num_records = seg.end_idx - seg.start_idx;
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

    const Key base_key = dataset[seg.start_idx];
    for (size_t j = seg.start_idx; j < seg.end_idx; ++j) {
      const size_t page_idx =
          PageForKey(base_key, seg.model->line(), seg.page_count, dataset[j]);
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
}

TEST(SegmentBuilderTest, Sequential_45_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(kSequentialKeys.size());
  for (const auto& key : kSequentialKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 45;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.Build(records);

  CheckSegments(kSequentialKeys, segments, goal, delta);
}

TEST(SegmentBuilderTest, Sequential_15_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(kSequentialKeys.size());
  for (const auto& key : kSequentialKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 15;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.Build(records);

  CheckSegments(kSequentialKeys, segments, goal, delta);
}

TEST(SegmentBuilderTest, Uniform_45_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(kUniformKeys.size());
  for (const auto& key : kUniformKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 45;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.Build(records);

  CheckSegments(kUniformKeys, segments, goal, delta);
}

TEST(SegmentBuilderTest, Uniform_15_5) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(kUniformKeys.size());
  for (const auto& key : kUniformKeys) {
    records.emplace_back(key, Slice());
  }

  const size_t goal = 15;
  const size_t delta = 5;
  SegmentBuilder builder(goal, delta);
  const auto segments = builder.Build(records);

  CheckSegments(kUniformKeys, segments, goal, delta);
}
