// Acknowledgement: The code in this file and in packed_map-inl.h is adapted
//                  from code originally written by Viktor Leis.

#pragma once

#include <cstddef>
#include <cstdint>

namespace tl {
namespace pg {

// An ordered map with a configurable compile-time fixed size that is strictly
// less than 64 KiB.
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
  // We want `sizeof(PackedMap<MapSizeBytes>) == MapSizeBytes` to be true to
  // make it easier to reason about this class. If the requested map size is not
  // divisible by 8 (should be architecture-specific), the compiler will add
  // extra padding to this class. For simplicity, we require `MapSizeBytes` to
  // be divisible by 8.
  static_assert(MapSizeBytes % 8 == 0,
                "The PackedMap must have a size that is divisible by 8.");

 public:
  // Create a map without key bounds. The `PackedMap` will not use prefix
  // encoding in this case.
  PackedMap() = default;

  // Create a map with a fixed key bound. This enables more efficient encoding
  // of later inserted keys, but requires that all inserted keys satisfy:
  // `lower_key` <= key <= `upper_key`. Using this constructor and then
  // violating this ordering constraint results in undefined behavior.
  // Can set upper_key_length = 0 to indicate an unknown upper key, forgoing
  // the more efficient encoding.
  PackedMap(const uint8_t* lower_key, unsigned lower_key_length,
            const uint8_t* upper_key, unsigned upper_key_length);

  // Insert `key` and `payload` and return true iff the insert succeeded.
  // Duplicate key insertions will just overwrite the payload.
  bool Insert(const uint8_t* key, unsigned key_length, const uint8_t* payload,
              unsigned payload_length);

  // Update `key`, which must already be in the map, with a new `payload` and
  // return true. If there is not enough space, then simply remove the pair
  // (`key`, old_payload) if it exists and return false.
  bool UpdateOrRemove(const uint8_t* key, unsigned key_length,
                      const uint8_t* payload, unsigned payload_length);

  // Insert `key` and `payload` and return true iff the insert succeeded. `key`
  // must be lexicographically greater than the largest key currently in the
  // map.
  //
  // If `perform_checks` is set to true, this key sorting condition will be
  // checked by calling CanAppend(), and Insert() or HandleDuplicateInsertion()
  // will be called instead if necessary. Otherwise, it is the user's
  // responsibility to maintain this requirement and undefined behavior will
  // result from violating it.
  //
  // Recommended for bulk insertions of unique keys in sorted order.
  //
  // Like Insert(), this function treats key re-insertions as overwrites. This
  // is consistent with what LevelDB/RocksDB do. To support non-unique keys, we
  // would need to decide on the desired behavior of Get() and Remove() when
  // dealing with keys with multiple associated values.
  bool Append(const uint8_t* key, unsigned key_length, const uint8_t* payload,
              unsigned payload_length, bool perform_checks);

  // Checks whether Append() can be used efficiently by lexicographically
  // comparing `key` to the largest key currently in the map.
  //
  // Returns `val` with:
  // -- val < 0 if `key` is smaller than the largest key currently in the map.
  // -- val == 0 if `key` is equal to the largest key currently in the map.
  // -- val > 0 if 'key` is larger than the largest key currently in the map.
  int8_t CanAppend(const uint8_t* key, unsigned key_length);

  // Remove the record associated with the specified key and return true if
  // the removal succeeded.
  bool Remove(const uint8_t* key, unsigned key_length);

  // Retrieve the payload for the specified key, returning true if found. If
  // the key is not found, `*payload_out` will be set to `nullptr` and
  // `*payload_length_out` will be set to 0.
  bool Get(const uint8_t* key, unsigned key_length, const uint8_t** payload_out,
           unsigned* payload_length_out) const;

  // Retrieve a pointer to the scratch storage area in the map. The scratch
  // space is 24 bytes large.
  const uint8_t* GetScratch() const;
  uint8_t* GetScratch();

  // Returns the number of records currently stored in this map.
  uint16_t GetNumRecords() const;

  // Retrieves the common prefix among all keys stored in this map.
  void GetKeyPrefix(const uint8_t** key_prefix_out,
                    unsigned* key_prefix_length_out) const;

  // Retrieve the key suffix stored in the given `slot_id`, returning true if
  // successful. Note that the full key is the prefix given by `GetKeyPrefix()`
  // concatenated with the suffix retrieved by this method.
  //
  // If there is no record at the provided `slot_id` (e.g., if `slot_id >=
  // GetNumRecords()`), this method will return false.
  bool GetKeySuffixInSlot(uint16_t slot_id, const uint8_t** key_suffix_out,
                          unsigned* key_suffix_length_out) const;

  // Retrieve the payload stored in the given `slot_id`, returning true if
  // successful.
  //
  // If there is no record at the provided `slot_id` (e.g., if `slot_id >=
  // GetNumRecords()`), this method will return false.
  bool GetPayloadInSlot(uint16_t slot_id, const uint8_t** payload_out,
                        unsigned* payload_length_out) const;

  // Finds the `slot_id` containing the first record with a key that is greater
  // than or equal to `key`.
  //
  // If no such record exists (e.g., if this map is empty or all record keys are
  // less than `key`), the returned `slot_id` will be greater than or equal to
  // the return value of `GetNumRecords()`.
  uint16_t LowerBoundSlot(const uint8_t* key, unsigned key_length) const;

  // Access the fences and their lengths - useful for creating a new PackedMap
  // with the same key range.
  const uint8_t* GetLowerFence() const;
  const uint8_t* GetUpperFence() const;
  const uint16_t GetLowerFenceLength() const;
  const uint16_t GetUpperFenceLength() const;

  // Check whether this is a valid PackedMap (as opposed to a PackedMap-sized
  // block of 0s).
  const bool IsValid() const;

  // Check whether this is an overflow page & make/unmake it one.
  const bool IsOverflow() const;
  void MakeOverflow();
  void UnmakeOverflow();

 private:
  static constexpr unsigned kHintCount = 16;
  static constexpr unsigned kScratchSize = 24;
  static_assert(sizeof(double) * 2 + sizeof(size_t) == kScratchSize);

  static constexpr uint8_t kValidFlag = 1;
  static constexpr uint8_t kOverflowFlag = 2;

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
    uint8_t scratch[kScratchSize];

    uint8_t flags = kValidFlag & ~kOverflowFlag;
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

  bool HandleDuplicateInsertion(uint16_t slot_id, const uint8_t* key,
                                unsigned key_length, const uint8_t* payload,
                                unsigned payload_length);
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

 public:
  static constexpr size_t kMaxKeySizeBytes =
      (MapSizeBytes - kTotalMetadataBytes - (2 * sizeof(Slot))) / 4;
  static constexpr size_t kUsableSize = MapSizeBytes - kTotalMetadataBytes;
  static constexpr size_t kSlotSize = sizeof(Slot);
};

}  // namespace pg
}  // namespace tl

#include "packed_map-inl.h"
