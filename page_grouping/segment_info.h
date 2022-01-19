#pragma once

#include <optional>

#include "persist/segment_id.h"
#include "plr/data.h"

namespace llsm {
namespace pg {

class SegmentInfo {
 public:
  SegmentInfo(SegmentId id, std::optional<plr::Line64> model)
      : id_(id), model_(model) {}

  // Represents an invalid `SegmentInfo`.
  SegmentInfo() : id_(), model_() {}

  SegmentId id() const { return id_; }
  size_t page_count() const { return 1ULL << id_.GetFileId(); }
  const std::optional<plr::Line64>& model() const { return model_; }

 private:
  SegmentId id_;
  std::optional<plr::Line64> model_;
};

}  // namespace pg
}  // namespace llsm
