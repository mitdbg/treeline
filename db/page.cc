#include "page.h"

#include <cstdint>
#include <limits>

#include "util/packed_map.h"

namespace {

// The theoretical maximum size of a `PackedMap` is currently 2^16 - 1 (64 KiB
// minus 1 byte) because it uses a 16-bit uint for offsets. As a result, the
// `PackedMap` cannot actually be exactly 64 KiB. If a `Page` is configured to
// be larger than what we can represent in a `PackedMap` (e.g., 64 KiB), we set
// the map's size to its largest possible size (2^16 - 8).
constexpr size_t MapSize = llsm::Page::kSize >
                                   std::numeric_limits<uint16_t>::max()
                               ? (1 << 16) - 8
                               : llsm::Page::kSize;

using PackedMap = llsm::PackedMap<MapSize>;
static_assert(sizeof(PackedMap) == MapSize);
static_assert(llsm::Page::kSize >= sizeof(PackedMap));

inline PackedMap* AsMapPtr(void* data) {
  return reinterpret_cast<PackedMap*>(data);
}

}  // namespace

namespace llsm {

Page::Page(void* data, const Slice& lower_key, const Slice& upper_key)
    : Page(data) {
  // This constructs a `PackedMap` in the memory pointed-to by `data_`. This
  // memory buffer must be large enough to hold a `PackedMap`.
  //
  // NOTE: Using "placement new" means that the object's destructor needs to be
  // manually called. But this is not a problem in our use case because
  // `PackedMap` does not have any members that use a custom destructor (i.e.,
  // we can get away with not calling `~PackedMap()` because there is nothing in
  // the class that needs "cleaning up").
  //
  // https://isocpp.org/wiki/faq/dtors#placement-new
  new(data_) ::PackedMap(
      reinterpret_cast<const uint8_t*>(lower_key.data()), lower_key.size(),
      reinterpret_cast<const uint8_t*>(upper_key.data()), upper_key.size());
}

Status Page::Put(const Slice& key, const Slice& value) {
  return Put(WriteOptions(), key, value);
}

Status Page::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  if (options.sorted_load) {
    if (!AsMapPtr(data_)->Append(reinterpret_cast<const uint8_t*>(key.data()),
                                 key.size(),
                                 reinterpret_cast<const uint8_t*>(value.data()),
                                 value.size(), options.perform_checks)) {
      return Status::InvalidArgument("Page is full.");
    }
  } else {
    if (!AsMapPtr(data_)->Insert(
            reinterpret_cast<const uint8_t*>(key.data()), key.size(),
            reinterpret_cast<const uint8_t*>(value.data()), value.size())) {
      return Status::InvalidArgument("Page is full.");
    }
  }
  return Status::OK();
}

Status Page::Get(const Slice& key, std::string* value_out) {
  const uint8_t* payload = nullptr;
  unsigned payload_length = 0;
  if (!AsMapPtr(data_)->Get(reinterpret_cast<const uint8_t*>(key.data()),
                            key.size(), &payload, &payload_length)) {
    return Status::NotFound("Key not found in page.");
  }

  value_out->clear();
  value_out->append(reinterpret_cast<const char*>(payload), payload_length);
  return Status::OK();
}

Status Page::Delete(const Slice& key) {
  if (!AsMapPtr(data_)->Remove(reinterpret_cast<const uint8_t*>(key.data()),
                               key.size())) {
    return Status::NotFound("Key not found in page.");
  }
  return Status::OK();
}

}  // namespace llsm
