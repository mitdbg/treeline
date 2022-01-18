#include "manager.h"

#include <iostream>

#include "segment_builder.h"

namespace llsm {
namespace pg {

Manager Manager::LoadIntoNew(
    std::filesystem::path db,
    const std::vector<std::pair<Key, const Slice>>& records) {
  // Temporary for testing.
  SegmentBuilder builder(/*goal=*/50, /*delta=*/10);
  const auto segments = builder.Build(records);
  std::cerr << "segment_page_count,num_records,model_slope,model_intercept" << std::endl;
  for (const auto& seg : segments) {
    std::cerr << seg.page_count << "," << seg.record_indices.size() << ",";
    if (seg.model.has_value()) {
      std::cerr << seg.model->line().slope() << "," << seg.model->line().intercept() << std::endl;
    } else {
      std::cerr << "," << std::endl;
    }
  }
  return Manager();
}

Manager Manager::Reopen(std::filesystem::path db) { return Manager(); }

Status Manager::Get(const Key& key, std::string* value_out) {
  return Status::OK();
}

Status Manager::PutBatch(
    const std::vector<std::pair<Key, const Slice>>& records) {
  return Status::OK();
}

Status Manager::Scan(const Key& start_key, const size_t amount,
                     std::vector<std::pair<Key, std::string>>* values_out) {
  return Status::OK();
}

}  // namespace pg
}  // namespace llsm
