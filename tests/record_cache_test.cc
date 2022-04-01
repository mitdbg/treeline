#include "gtest/gtest.h"

#define private public
#include "../record_cache/record_cache.h"

namespace {

using namespace tl;

TEST(RecordCacheTest, SimplePutGet) {
  const uint64_t capacity = 5;
  auto rc = RecordCache(capacity);
  Slice key = "aaa";
  Slice value = "bbb";

  rc.Put(key, value);

  uint64_t index_out;
  ASSERT_TRUE(rc.GetCacheIndex(key, false, &index_out).ok());
  ASSERT_EQ(value.compare(rc.cache_entries[index_out].GetValue()), 0);
  rc.cache_entries[index_out].Unlock();
}

#ifndef NDEBUG
TEST(RecordCacheTest, SimplePutDuplicate) {
  const uint64_t capacity = 5;
  auto rc = RecordCache(capacity);
  Slice key = "aaa";
  Slice value1 = "bbb";
  Slice value2 = "ccc";

  ASSERT_TRUE(rc.Put(key, value1, /*is_dirty = */ true).ok());
  rc.override = true;
  ASSERT_TRUE(rc.Put(key, value2, /*is_dirty = */ true).ok());

  uint64_t index_out;
  ASSERT_TRUE(rc.GetCacheIndex(key, false, &index_out).ok());
  ASSERT_EQ(value2.compare(rc.cache_entries[index_out].GetValue()), 0);
  rc.cache_entries[index_out].Unlock();
}
#endif

TEST(RecordCacheTest, SimpleMiss) {
  const uint64_t capacity = 5;
  auto rc = RecordCache(capacity);
  Slice key = "aaa";

  uint64_t index_out;
  ASSERT_TRUE(rc.GetCacheIndex(key, false, &index_out).IsNotFound());
}

TEST(RecordCacheTest, PutFromRead) {
  const uint64_t capacity = 5;
  auto rc = RecordCache(capacity);
  Slice key = "aaa";
  Slice value = "bbb";

  rc.PutFromRead(key, value);

  uint64_t index_out;

  ASSERT_TRUE(rc.GetCacheIndex(key, false, &index_out).ok());
  ASSERT_EQ(value.compare(rc.cache_entries[index_out].GetValue()), 0);
  ASSERT_FALSE(rc.cache_entries[index_out].IsDirty());
  rc.cache_entries[index_out].Unlock();
}

TEST(RecordCacheTest, MultiPutGet) {
  const uint64_t capacity = 10;
  auto rc = RecordCache(capacity);

  for (auto i = 100; i < 200; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s));
  }

  uint64_t index_out;

  for (auto i = 100; i < 190; ++i) {
    Slice key("a" + std::to_string(i));
    ASSERT_TRUE(rc.GetCacheIndex(key, false, &index_out).IsNotFound());
  }

  for (auto i = 190; i < 200; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);

    ASSERT_TRUE(rc.GetCacheIndex(Slice(key_s), false, &index_out).ok());
    ASSERT_EQ(Slice(val_s).compare(rc.cache_entries[index_out].GetValue()), 0);
    rc.cache_entries[index_out].Unlock();
  }
}

TEST(RecordCacheTest, RangeScanWithLength) {
  const uint64_t capacity = 100;
  auto rc = RecordCache(capacity);

  for (auto i = 100; i < 200; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s));
  }

  uint64_t index_out;
  uint64_t scan_length = 10;
  std::vector<uint64_t> results;

  for (auto i = 100; i < 200; ++i) {
    std::string start_key_s = "a" + std::to_string(i);

    ASSERT_TRUE(rc.GetRange(Slice(start_key_s), scan_length, &results).ok());
    uint64_t scanned_recs = results.size();
    ASSERT_EQ(scanned_recs, i < 200 - scan_length ? scan_length : 200 - i);

    for (auto j = 0; j < scanned_recs; ++j) {
      std::string key_s = "a" + std::to_string(i + j);
      std::string val_s = "b" + std::to_string(i + j);

      auto entry = &rc.cache_entries[results[j]];

      ASSERT_EQ(Slice(key_s).compare(entry->GetKey()), 0);
      ASSERT_EQ(Slice(val_s).compare(entry->GetValue()), 0);

      entry->Unlock();
    }
  }
}

TEST(RecordCacheTest, RangeScanWithBound) {
  const uint64_t capacity = 100;
  auto rc = RecordCache(capacity);

  for (auto i = 100; i < 150; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s));
  }

  uint64_t index_out;
  uint64_t scan_length = 10;
  std::vector<uint64_t> results;

  for (auto i = 100; i < 150; ++i) {
    std::string start_key_s = "a" + std::to_string(i);
    std::string end_key_s = "a" + std::to_string(i + scan_length);

    ASSERT_TRUE(rc.GetRangeImpl(Slice(start_key_s), Slice(end_key_s), &results,
                                std::nullopt)
                    .ok());
    uint64_t scanned_recs = results.size();
    ASSERT_EQ(scanned_recs, i < 150 - scan_length ? scan_length : 150 - i);

    for (auto j = 0; j < scanned_recs; ++j) {
      std::string key_s = "a" + std::to_string(i + j);
      std::string val_s = "b" + std::to_string(i + j);

      auto entry = &rc.cache_entries[results[j]];

      ASSERT_EQ(Slice(key_s).compare(entry->GetKey()), 0);
      ASSERT_EQ(Slice(val_s).compare(entry->GetValue()), 0);

      entry->Unlock();
    }

    results.clear();
  }
}

TEST(RecordCacheTest, PreferNonDirtyEviction) {
  const uint64_t capacity = 10;
  auto rc = RecordCache(capacity);

  // Insert 10 records.
  for (auto i = 100; i < 110; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s), /*is_dirty = */ false);
  }
  ASSERT_EQ(rc.clock_ % rc.capacity_, 0);

  // Make the first 5 dirty.
  for (auto i = 100; i < 105; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "c" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s), /*is_dirty = */ true);
  }
  ASSERT_EQ(rc.clock_ % rc.capacity_, 0);

  // Insert 5 new records.
  for (auto i = 110; i < 115; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s), /*is_dirty = */ false);
  }
  ASSERT_EQ(rc.clock_ % rc.capacity_, 0);

  // Check that the dirty records are still there, unlike the clean ones.
  for (auto i = 100; i < 105; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "c" + std::to_string(i);

    uint64_t index_out;
    ASSERT_TRUE(rc.GetCacheIndex(Slice(key_s), false, &index_out).ok());
    ASSERT_EQ(Slice(val_s).compare(rc.cache_entries[index_out].GetValue()), 0);
    rc.cache_entries[index_out].Unlock();
  }
  for (auto i = 105; i < 110; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);

    uint64_t index_out;
    ASSERT_FALSE(rc.GetCacheIndex(Slice(key_s), false, &index_out).ok());
  }
}

}  // namespace
