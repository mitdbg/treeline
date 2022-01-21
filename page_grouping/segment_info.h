#pragma once

#include <optional>

#include "key.h"
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

  size_t PageForKey(Key base_key, Key candidate) const {
    const size_t pages = page_count();
    if (pages == 1) return 0;
    return pg::PageForKey(base_key, *model_, pages, candidate);
  }

 private:
  SegmentId id_;
  std::optional<plr::Line64> model_;
};

}  // namespace pg
}  // namespace llsm
