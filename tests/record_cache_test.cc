#include "../record_cache/record_cache.h"

#include "gtest/gtest.h"

namespace {

using namespace llsm;

TEST(RecordCacheTest, SimplePutGet) {
  auto rc = RecordCache(5);
  Slice key = "aaa";
  Slice value = "bbb";

  rc.Put(key, value);

  uint64_t index_out;
  ASSERT_TRUE(rc.GetIndex(key, &index_out).ok());
  ASSERT_EQ(value.compare(rc.cache_entries[index_out].GetValue()), 0);
}

TEST(RecordCacheTest, SimpleMiss) {
  auto rc = RecordCache(5);
  Slice key = "aaa";

  uint64_t index_out;
  ASSERT_TRUE(rc.GetIndex(key, &index_out).IsNotFound());
}

TEST(RecordCacheTest, PutVariants) {
  auto rc = RecordCache(5);
  Slice key1 = "aa1";
  Slice key2 = "aa2";
  Slice key3 = "aa3";
  Slice value = "bbb";

  rc.PutFromWrite(key1, value);

  uint64_t index_out;
  ASSERT_TRUE(rc.GetIndex(key1, &index_out).ok());
  ASSERT_EQ(value.compare(rc.cache_entries[index_out].GetValue()), 0);
  ASSERT_TRUE(rc.cache_entries[index_out].IsDirty());
  ASSERT_TRUE(rc.cache_entries[index_out].IsWrite());

  rc.PutFromRead(key2, value);

  ASSERT_TRUE(rc.GetIndex(key2, &index_out).ok());
  ASSERT_EQ(value.compare(rc.cache_entries[index_out].GetValue()), 0);
  ASSERT_FALSE(rc.cache_entries[index_out].IsDirty());

  rc.PutFromDelete(key3);

  ASSERT_TRUE(rc.GetIndex(key3, &index_out).ok());
  ASSERT_EQ(Slice().compare(rc.cache_entries[index_out].GetValue()), 0);
  ASSERT_TRUE(rc.cache_entries[index_out].IsDirty());
  ASSERT_TRUE(rc.cache_entries[index_out].IsDelete());
}

TEST(RecordCacheTest, MultiPutGet) {
  auto rc = RecordCache(10);

  for (auto i = 100; i < 200; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);
    rc.Put(Slice(key_s), Slice(val_s));
  }

  uint64_t index_out;

  for (auto i = 100; i < 190; ++i) {
    Slice key("a" + std::to_string(i));
    ASSERT_TRUE(rc.GetIndex(key, &index_out).IsNotFound());
  }

  for (auto i = 190; i < 200; ++i) {
    std::string key_s = "a" + std::to_string(i);
    std::string val_s = "b" + std::to_string(i);

    ASSERT_TRUE(rc.GetIndex(Slice(key_s), &index_out).ok());
    ASSERT_EQ(Slice(val_s).compare(rc.cache_entries[index_out].GetValue()), 0);
  }
}

}  // namespace
