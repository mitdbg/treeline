#include <algorithm>
#include <cstdint>
#include <iostream>
#include <utility>
#include <vector>

#include "bench/common/load_data.h"
#include "gflags/gflags.h"
#include "key.h"
#include "llsm/slice.h"
#include "segment_builder.h"

DEFINE_string(custom_dataset, "", "A path to a custom dataset.");
DEFINE_uint32(goal, 45, "Records per page goal.");
DEFINE_uint32(delta, 10, "Records per page delta.");

using namespace llsm;
using namespace llsm::pg;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  const size_t max_records_per_page = FLAGS_goal + 2 * FLAGS_delta;
  const size_t min_records_per_page = FLAGS_goal - 2 * FLAGS_delta;

  std::vector<uint64_t> keys = llsm::bench::LoadDatasetFromTextFile(
      FLAGS_custom_dataset, /*warn_on_duplicates=*/true);
  std::sort(keys.begin(), keys.end());
  std::cerr << "> Loaded a custom dataset with " << keys.size() << " keys."
            << std::endl;

  std::vector<std::pair<uint64_t, Slice>> dataset;
  dataset.reserve(keys.size());
  for (const auto& key : keys) {
    dataset.emplace_back(key << 16, Slice());
  }

  SegmentBuilder builder(FLAGS_goal, FLAGS_delta);
  const auto segments = builder.Build(dataset);

  // Assign records to "pages" and validate the fill proportion.
  for (size_t seg_id = 0; seg_id < segments.size(); ++seg_id) {
    const auto& seg = segments[seg_id];
    if (seg.page_count == 1) continue;

    const auto base_key = dataset[seg.start_idx].first;
    size_t curr_page = 0;
    size_t recs_in_page = 0;
    for (size_t i = seg.start_idx; i < seg.end_idx; ++i) {
      const auto& rec = dataset[i];
      const size_t assigned_page =
          PageForKey(base_key, seg.model->line(), seg.page_count, rec.first);
      if (assigned_page == curr_page) {
        recs_in_page++;
      } else {
        // Validate the old page size.
        if (recs_in_page > max_records_per_page ||
            recs_in_page < min_records_per_page) {
          std::cerr << "Overfull or underfull page " << curr_page
                    << " in segment " << seg_id
                    << " (segment size: " << seg.page_count
                    << "). Size: " << recs_in_page << std::endl;
        }
        curr_page = assigned_page;
        recs_in_page = 1;
      }
    }
    if (recs_in_page > max_records_per_page ||
        recs_in_page < min_records_per_page) {
      std::cerr << "Overfull or underfull page " << curr_page << " in segment "
                << seg_id << " (segment size: " << seg.page_count
                << "). Size: " << recs_in_page << std::endl;
    }
  }

  std::cerr << "> Done validation." << std::endl;

  return 0;
}