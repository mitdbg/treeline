#include "gtest/gtest.h"

#include <algorithm>
#include <vector>

#include "llsm/status.h"
#include "db/memtable.h"

namespace {

using namespace llsm;

TEST(MemTableTest, SimpleReadWrite) {
  MemTable table;
  std::string key = "hello";
  std::string value = "world";

  Status s = table.Put(key, value);
  ASSERT_TRUE(s.ok());

  std::string value_out;
  MemTable::EntryType entry_type_out;
  s = table.Get(key, &entry_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(entry_type_out, MemTable::EntryType::kWrite);
  ASSERT_EQ(value, value_out);
}

TEST(MemTableTest, NegativeLookup) {
  MemTable table;
  std::string key = "hello";

  std::string value_out;
  MemTable::EntryType entry_type_out;
  Status s = table.Get(key, &entry_type_out, &value_out);
  ASSERT_TRUE(s.IsNotFound());
  s = table.Delete(key);
  ASSERT_TRUE(s.ok());
  s = table.Get(key, &entry_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(entry_type_out, MemTable::EntryType::kDelete);
}

TEST(MemTableTest, WriteThenDelete) {
  MemTable table;
  std::string key = "hello";
  std::string value = "world";

  Status s = table.Put(key, value);
  ASSERT_TRUE(s.ok());

  std::string value_out;
  MemTable::EntryType entry_type_out;
  s = table.Get(key, &entry_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(entry_type_out, MemTable::EntryType::kWrite);
  ASSERT_EQ(value, value_out);

  s = table.Delete(key);
  ASSERT_TRUE(s.ok());
  s = table.Get(key, &entry_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(entry_type_out, MemTable::EntryType::kDelete);
}

TEST(MemTableTest, DeleteThenWrite) {
  MemTable table;
  std::string key = "hello";
  std::string value = "world";

  Status s = table.Delete(key);
  ASSERT_TRUE(s.ok());

  std::string value_out;
  MemTable::EntryType entry_type_out;
  s = table.Get(key, &entry_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(entry_type_out, MemTable::EntryType::kDelete);

  s = table.Put(key, value);
  ASSERT_TRUE(s.ok());
  s = table.Get(key, &entry_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(entry_type_out, MemTable::EntryType::kWrite);
  ASSERT_EQ(value, value_out);
}

TEST(MemTableTest, InOrderIterate) {
  std::vector<std::string> strs = {"hello", "world", "ordering", "alphabet"};
  std::vector<std::string> ordered(strs);
  std::sort(ordered.begin(), ordered.end());

  std::string value = "test string";
  MemTable table;
  for (const auto& s : strs) {
    ASSERT_TRUE(table.Put(s, value).ok());
  }

  size_t idx = 0;
  for (const auto& kv : table) {
    ASSERT_TRUE(idx < ordered.size());
    ASSERT_TRUE(kv.first.compare(ordered.at(idx)) == 0);
    ASSERT_TRUE(kv.second.first.compare(value) == 0);
    ASSERT_EQ(kv.second.second, MemTable::EntryType::kWrite);
    ++idx;
  }
}

}  // namespace
