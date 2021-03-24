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

constexpr size_t Page::UsableSize() { return ::PackedMap::kUsableSize; }

constexpr size_t Page::PerRecordMetadataSize() {
  return ::PackedMap::kSlotSize;
}

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
  new (data_)::PackedMap(
      reinterpret_cast<const uint8_t*>(lower_key.data()), lower_key.size(),
      reinterpret_cast<const uint8_t*>(upper_key.data()), upper_key.size());
}

Slice Page::GetKeyPrefix() const {
  const uint8_t* prefix = nullptr;
  unsigned length = 0;
  AsMapPtr(data_)->GetKeyPrefix(&prefix, &length);
  return Slice(reinterpret_cast<const char*>(prefix), length);
}

Status Page::Put(const Slice& key, const Slice& value) {
  return Put(WriteOptions(), key, value);
}

Status Page::Put(const WriteOptions& options, const Slice& key,
                 const Slice& value) {
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

Page::Iterator Page::GetIterator() const { return Iterator(*this); }

Page::Iterator::Iterator(const Page& page)
    : page_(page),
      current_slot_(0),
      prefix_length_(0),
      key_buffer_valid_(false) {
  const Slice prefix = page_.GetKeyPrefix();
  key_buffer_.append(prefix.data(), prefix.size());
  prefix_length_ = prefix.size();
  assert(prefix_length_ == key_buffer_.size());
}

void Page::Iterator::Seek(const Slice& key) {
  current_slot_ =
      AsMapPtr(page_.data_)
          ->LowerBoundSlot(reinterpret_cast<const uint8_t*>(key.data()),
                           key.size());
  key_buffer_valid_ = false;
}

void Page::Iterator::Next() {
  ++current_slot_;
  key_buffer_valid_ = false;
}

bool Page::Iterator::Valid() const {
  return current_slot_ < AsMapPtr(page_.data_)->GetNumRecords();
}

size_t Page::Iterator::RecordsLeft() const {
  const size_t num_records = AsMapPtr(page_.data_)->GetNumRecords();
  if (current_slot_ > num_records) return 0;
  return num_records - current_slot_;
}

Slice Page::Iterator::key() const {
  if (!key_buffer_valid_) {
    key_buffer_.resize(prefix_length_);
    const uint8_t* suffix = nullptr;
    unsigned length = 0;
    const bool found =
        AsMapPtr(page_.data_)
            ->GetKeySuffixInSlot(current_slot_, &suffix, &length);
    assert(found);
    key_buffer_.append(reinterpret_cast<const char*>(suffix), length);
    key_buffer_valid_ = true;
  }
  return Slice(key_buffer_.data(), key_buffer_.size());
}

Slice Page::Iterator::value() const {
  assert(Valid());
  const uint8_t* value = nullptr;
  unsigned length = 0;
  const bool found =
      AsMapPtr(page_.data_)->GetPayloadInSlot(current_slot_, &value, &length);
  assert(found);
  return Slice(reinterpret_cast<const char*>(value), length);
}

}  // namespace llsm
