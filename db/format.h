#pragma once

#include <cstdint>

namespace tl {
namespace format {

// Numeric values in this file should not be changed because they are part of
// TL's on-disk format.

// Used to disambiguate between inserting/updating a record and deleting a
// record (both of which are treated as "writes").
enum class WriteType : uint8_t {
  kWrite = 0,
  kDelete = 1
};
inline constexpr uint8_t kMaxWriteType = static_cast<uint8_t>(WriteType::kDelete);

}  // namespace format
}  // namespace tl
