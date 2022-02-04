#include "../record_cache/record_cache.h"

#include "gtest/gtest.h"

namespace {

using namespace llsm;

TEST(RecordCacheTest, SimplePutGet) {
  Options options;
  options.record_cache_capacity = 5;
  Statistics stats;
  auto rc = RecordCache(&options, &stats);
  Slice key = "aaa";
  Slice value = "bbb";

  rc.Put(key, value);

  uint64_t index_out;
  ASSERT_TRUE(rc.GetCacheIndex(key, false, &index_out).ok());
  ASSERT_EQ(value.compare(rc.cache_entries[index_out].GetValue()), 0);
  rc.cache_entries[index_out].Unlock();
}

TEST(RecordCacheTest, SimpleMiss) {
  Options options;
  options.record_cache_capacity = 5;
  Statistics stats;
  auto rc = RecordCache(&options, &stats);
  Slice key = "aaa";

  uint64_t index_out;
  ASSERT_TRUE(rc.GetCacheIndex(key, false, &index_out).IsNotFound());
}

TEST(RecordCacheTest, PutFromRead) {
  Options options;
  options.record_cache_capacity = 5;
  Statistics stats;
  auto rc = RecordCache(&options, &stats);
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
  Options options;
  options.record_cache_capacity = 10;
  Statistics stats;
  auto rc = RecordCache(&options, &stats);

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

TEST(RecordCacheTest, RangeScan) {
  Options options;
  options.record_cache_capacity = 100;
  Statistics stats;
  auto rc = RecordCache(&options, &stats);

  for (auto i = 100; i < 200; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s));
  }

  uint64_t index_out;
  uint64_t scan_length = 10;
  uint64_t results[scan_length];

  for (auto i = 100; i < 200; ++i) {
    std::string start_key_s = "a" + std::to_string(i);

    uint64_t scanned_recs;

    ASSERT_TRUE(
        rc.GetRange(Slice(start_key_s), scan_length, results, scanned_recs)
            .ok());
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

}  // namespace
