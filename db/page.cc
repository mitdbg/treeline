#include "page.h"

#include <cstring>

namespace llsm {

Status Page::Put(const Slice& key, const Slice& value) {
  if (!rep_.Insert(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                   reinterpret_cast<const uint8_t*>(value.data()),
                   value.size())) {
    return Status::InvalidArgument("Page is full.");
  }
  return Status::OK();
}

Status Page::Get(const Slice& key, std::string* value_out) {
  const uint8_t* payload = nullptr;
  unsigned payload_length = 0;
  if (!rep_.Get(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                &payload, &payload_length)) {
    return Status::NotFound("Key not found in page.");
  }

  value_out->clear();
  value_out->append(reinterpret_cast<const char*>(payload), payload_length);
  return Status::OK();
}

Status Page::Delete(const Slice& key) {
  if (!rep_.Remove(reinterpret_cast<const uint8_t*>(key.data()), key.size())) {
    return Status::NotFound("Key not found in page.");
  }
  return Status::OK();
}

Status Page::LoadFrom(const Slice& buf) {
  if (buf.size() < kSize) {
    return Status::InvalidArgument("Buffer too small to hold a page.");
  }
  memcpy(&rep_, buf.data(), kSize);
  return Status::OK();
}

void Page::EncodeTo(std::string& dest) const {
  dest.append(reinterpret_cast<const char*>(&rep_), kSize);
}

}  // namespace llsm
