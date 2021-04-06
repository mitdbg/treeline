#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "bufmgr/logical_page_id.h"

namespace llsm {

class Slice;
class Status;

// A model to determine the correct page for a database record based on the key.
class Model {
 public:
  virtual ~Model() = default;

  // Uses the model to derive a logical page_id given a `key`.
  virtual LogicalPageId KeyToPageId(const Slice& key) const = 0;

  // Serializes the model and appends it to `dest`.
  virtual void EncodeTo(std::string* dest) const = 0;

  // Parses a `Model` from `source` and advances `source` past the parsed bytes.
  //
  // If the parse is successful, `status_out` will be OK and the returned
  // pointer will be non-null. If the parse is unsuccessful, `status_out` will
  // be non-OK and the returned pointer will be `nullptr`.
  static std::unique_ptr<Model> LoadFrom(Slice* source, Status* status_out);
};

namespace detail {

// Used for de/serialization purposes only.
enum class ModelType : uint8_t { kDirectModel = 0, kRSModel = 1 };

inline constexpr uint8_t kMaxModelType =
    static_cast<uint8_t>(ModelType::kRSModel);

}  // namespace detail
}  // namespace llsm
