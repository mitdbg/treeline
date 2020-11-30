// Acknowledgement: The code in this file and in packed_map-inl.h is adapted
//                  from code originally written by Viktor Leis.

#pragma once

#include <cstdint>

namespace llsm {

// An ordered map with a configurable compile-time fixed size of at most 64 kiB.
//
// Keys and payloads are treated as arbitrary bytes. This map keeps keys ordered
// lexicographically. Keys have a maximum size, specified by
// `PackedMap<>::kMaxKeySizeBytes`. The key range must also be specified up
// front to leverage prefixed encoding.
//
// Since this map can become full, it provides an eight byte buffer to store
// optional "overflow" information (e.g., a pointer, offset, or identifier for
// an additional map). This buffer must be managed by the map user.
//
// This map is not thread safe; it requires external mutual exclusion.
template <uint16_t MapSizeBytes>
class PackedMap {
 public:
  // Create a map without key bounds. The `PackedMap` will not use prefix
  // encoding in this case.
  PackedMap() = default;

  // Create a map with a fixed key bound. This enables more efficient encoding
  // of later inserted keys, but requires that all inserted keys satisfy:
  // `lower_key` <= key <= `upper_key`. Using this constructor and then
  // violating this ordering constraint results in undefined behavior.
  PackedMap(const uint8_t* lower_key, unsigned lower_key_length,
            const uint8_t* upper_key, unsigned upper_key_length);

  // Insert `key` and `payload` and return true if the insert succeeded.
  // Duplicate key insertions will just override the payload.
  bool Insert(const uint8_t* key, unsigned key_length, const uint8_t* payload,
              unsigned payload_length);

  // Remove the record associated with the specified key and return true if
  // the removal succeeded.
  bool Remove(const uint8_t* key, unsigned key_length);

  // Retrieve the payload for the specified key, returning true if found. If
  // the key is not found, `*payload_out` will be set to `nullptr` and
  // `*payload_length_out` will be set to 0.
  bool Get(const uint8_t* key, unsigned key_length, const uint8_t** payload_out,
           unsigned* payload_length_out) const;

  // Retrieve the stored `overflow` (an arbitrary 8 byte value managed by
  // the map user).
  uint64_t GetOverflow() const;

  // Set the stored overflow value to `overflow` (an arbitrary 8 byte value
  // managed by the map user).
  void SetOverflow(uint64_t overflow);

 private:
  static constexpr unsigned kHintCount = 16;
  struct Header {
    struct FenceKeySlot {
      uint16_t offset;
      uint16_t length;
    };
    // If non-zero, these fences delineate the key space in this map:
    // [lower_fence, upper_fence]. That is, all keys in this map must satisfy
    // lower_fence <= key <= upper_fence.
    FenceKeySlot lower_fence = {0, 0};
    FenceKeySlot upper_fence = {0, 0};

    uint16_t count = 0;
    uint16_t space_used = 0;
    uint16_t data_offset = MapSizeBytes;
    uint16_t prefix_length = 0;

    uint32_t hint[kHintCount];
    uint64_t overflow = 0;
  };
  struct Slot {
    uint16_t offset;
    uint16_t key_length;
    uint16_t payload_length;
    union {
      uint32_t head;
      uint8_t head_bytes[4];
    };
  } __attribute__((packed));

  Header header_;
  uint8_t padding_[(MapSizeBytes - sizeof(Header)) % sizeof(Slot)];
  static constexpr unsigned kTotalMetadataBytes =
      sizeof(Header) + sizeof(padding_);
  union {
    // Grows from front
    Slot slot_[(MapSizeBytes - kTotalMetadataBytes) / sizeof(Slot)];
    // Grows from back
    uint8_t heap_[MapSizeBytes - kTotalMetadataBytes];
  };

  // For convenience we want the page payload size to be divisible by the Slot
  // size. This assertion ensures we are using an appropriate amount padding
  // in the header.
  static_assert((MapSizeBytes - kTotalMetadataBytes) % sizeof(Slot) == 0);

  // Maps cannot be smaller than the header (plus any needed padding).
  static_assert(MapSizeBytes > kTotalMetadataBytes,
                "The PackedMap must be large enough to store its header.");

  uint8_t* Ptr();
  const uint8_t* Ptr() const;
  const uint8_t* GetPrefix() const;

  void StoreKeyValue(uint16_t slot_id, const uint8_t* key, unsigned key_length,
                     const uint8_t* payload, unsigned payload_length);
  bool RemoveSlot(unsigned slot_id);

  unsigned SpaceNeeded(unsigned key_length, unsigned payload_length) const;
  bool RequestSpaceFor(unsigned space_needed);
  unsigned FreeSpace() const;
  unsigned FreeSpaceAfterCompaction() const;
  void Compactify();
  void RemoveFromHeapAndCompact(unsigned slot_id);
  void CopyKeyValueRange(PackedMap<MapSizeBytes>& dst, uint16_t dst_slot,
                         uint16_t src_slot, unsigned src_count);
  void CopyKeyValue(uint16_t src_slot, PackedMap<MapSizeBytes>& dst,
                    uint16_t dst_slot);

  void SetFences(const uint8_t* lower_key, unsigned lower_length,
                 const uint8_t* upper_key, unsigned upper_length);
  void InsertFence(typename Header::FenceKeySlot& fk, const uint8_t* key,
                   unsigned key_length);

  void MakeHint();
  void SearchHint(uint32_t key_head, unsigned& lower_out,
                  unsigned& upper_out) const;
  void UpdateHint(unsigned slot_id);
  unsigned LowerBound(const uint8_t* key, unsigned key_length,
                      bool& found_out) const;

  uint8_t* GetKey(unsigned slot_id);
  uint8_t* GetPayload(unsigned slot_id);
  const uint8_t* GetKey(unsigned slot_id) const;
  const uint8_t* GetPayload(unsigned slot_id) const;
  const uint8_t* GetLowerFence() const;
  const uint8_t* GetUpperFence() const;

 public:
  static constexpr size_t kMaxKeySizeBytes =
      (MapSizeBytes - kTotalMetadataBytes - (2 * sizeof(Slot))) / 4;
};

}  // namespace llsm

#include "packed_map-inl.h"
