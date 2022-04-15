#include "db/memtable.h"

#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "treeline/status.h"

namespace {

using namespace tl;
using namespace tl::format;

TEST(MemTableTest, SimpleReadWrite) {
  MemTableOptions moptions;
  MemTable table(moptions);
  std::string key = "hello";
  std::string value = "world";

  Status s = table.Put(key, value);
  ASSERT_TRUE(s.ok());

  std::string value_out;
  WriteType write_type_out;
  s = table.Get(key, &write_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(write_type_out, WriteType::kWrite);
  ASSERT_EQ(value, value_out);
}

TEST(MemTableTest, NegativeLookup) {
  MemTableOptions moptions;
  MemTable table(moptions);
  std::string key = "hello";

  std::string value_out;
  WriteType write_type_out;
  Status s = table.Get(key, &write_type_out, &value_out);
  ASSERT_TRUE(s.IsNotFound());
  s = table.Delete(key);
  ASSERT_TRUE(s.ok());
  s = table.Get(key, &write_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(write_type_out, WriteType::kDelete);
}

TEST(MemTableTest, WriteThenDelete) {
  MemTableOptions moptions;
  MemTable table(moptions);
  std::string key = "hello";
  std::string value = "world";

  Status s = table.Put(key, value);
  ASSERT_TRUE(s.ok());

  std::string value_out;
  WriteType write_type_out;
  s = table.Get(key, &write_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(write_type_out, WriteType::kWrite);
  ASSERT_EQ(value, value_out);

  s = table.Delete(key);
  ASSERT_TRUE(s.ok());
  s = table.Get(key, &write_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(write_type_out, WriteType::kDelete);
}

TEST(MemTableTest, DeleteThenWrite) {
  MemTableOptions moptions;
  MemTable table(moptions);
  std::string key = "hello";
  std::string value = "world";

  Status s = table.Delete(key);
  ASSERT_TRUE(s.ok());

  std::string value_out;
  WriteType write_type_out;
  s = table.Get(key, &write_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(write_type_out, WriteType::kDelete);

  s = table.Put(key, value);
  ASSERT_TRUE(s.ok());
  s = table.Get(key, &write_type_out, &value_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(write_type_out, WriteType::kWrite);
  ASSERT_EQ(value, value_out);
}

TEST(MemTableTest, InOrderIterate) {
  std::vector<std::string> strs = {"hello", "world", "ordering", "alphabet"};
  std::vector<std::string> ordered(strs);
  std::sort(ordered.begin(), ordered.end());

  std::string value = "test string";
  MemTableOptions moptions;
  MemTable table(moptions);
  for (const auto& s : strs) {
    ASSERT_TRUE(table.Put(s, value).ok());
  }

  size_t idx = 0;
  MemTable::Iterator it = table.GetIterator();
  it.SeekToFirst();
  ASSERT_TRUE(it.Valid());
  for (; it.Valid(); it.Next(), ++idx) {
    ASSERT_TRUE(idx < ordered.size());
    ASSERT_TRUE(it.key().compare(ordered.at(idx)) == 0);
    ASSERT_TRUE(it.value().compare(value) == 0);
    ASSERT_EQ(it.type(), WriteType::kWrite);
  }

  // All strings should have been found
  ASSERT_EQ(idx, ordered.size());
}

TEST(MemTableTest, MultiWriteIterate) {
  MemTableOptions moptions;
  MemTable table(moptions);
  const std::string final_value = "test string";
  ASSERT_TRUE(table.Put("abc", "hello").ok());
  ASSERT_TRUE(table.Put("xyz123", "world").ok());
  ASSERT_TRUE(table.Put("xyz123abc", "test").ok());
  ASSERT_TRUE(table.Put("test key", final_value).ok());
  ASSERT_TRUE(table.Put("abcde", "another test string").ok());

  // Make sure our iterator behaves properly on "duplicate" key writes.
  ASSERT_TRUE(table.Put("xyz123", "replaced1").ok());
  ASSERT_TRUE(table.Put("xyz123", "replaced2").ok());
  ASSERT_TRUE(table.Put("xyz123", "replaced3").ok());
  ASSERT_TRUE(table.Put("xyz123", "replaced4").ok());
  ASSERT_TRUE(table.Put("xyz123", "replaced5").ok());
  ASSERT_TRUE(table.Put("xyz123", final_value).ok());
  ASSERT_TRUE(table.Delete("abc").ok());
  ASSERT_TRUE(table.Put("abcde", final_value).ok());
  ASSERT_TRUE(table.Put("abcd", final_value).ok());
  ASSERT_TRUE(table.Put("xyz123abc", final_value).ok());

  MemTable::Iterator it = table.GetIterator();

  // Delete: abc
  it.SeekToFirst();
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare("abc") == 0);
  ASSERT_EQ(it.type(), WriteType::kDelete);

  // Write: (abcd, test string)
  it.Next();
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare("abcd") == 0);
  ASSERT_EQ(it.type(), WriteType::kWrite);
  ASSERT_TRUE(it.value().compare(final_value) == 0);

  // Write: (abcde, test string)
  it.Next();
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare("abcde") == 0);
  ASSERT_EQ(it.type(), WriteType::kWrite);
  ASSERT_TRUE(it.value().compare(final_value) == 0);

  // Write: (test key, test string)
  it.Next();
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare("test key") == 0);
  ASSERT_EQ(it.type(), WriteType::kWrite);
  ASSERT_TRUE(it.value().compare(final_value) == 0);

  // Write: (xyz123, test string)
  it.Next();
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare("xyz123") == 0);
  ASSERT_EQ(it.type(), WriteType::kWrite);
  ASSERT_TRUE(it.value().compare(final_value) == 0);

  // Write: (xyz123abc, test string)
  it.Next();
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare("xyz123abc") == 0);
  ASSERT_EQ(it.type(), WriteType::kWrite);
  ASSERT_TRUE(it.value().compare(final_value) == 0);

  // End of table
  it.Next();
  ASSERT_FALSE(it.Valid());
}

TEST(MemTableTest, SimilarGet) {
  MemTableOptions moptions;
  MemTable table(moptions);
  std::string value_out;
  WriteType write_type_out;
  ASSERT_TRUE(table.Put("abcd", "hello").ok());
  ASSERT_TRUE(table.Get("abc", &write_type_out, &value_out).IsNotFound());
}

TEST(MemTableTest, AddFromDeferral) {
  MemTableOptions moptions;
  MemTable table(moptions);

  ASSERT_TRUE(table.Put("abcd", "hello1").ok());
  ASSERT_TRUE(table
                  .Add("abcd", "hello2", WriteType::kWrite,
                       /*from_deferral = */ true,
                       /*injected_sequence_number = */ 0)
                  .ok());

  // Check that you don't read it out
  std::string value_out;
  WriteType write_type_out;
  Status s = table.Get("abcd", &write_type_out, &value_out);
  ASSERT_EQ(value_out, "hello1");

  // Check that it's skipped by the iterator
  ASSERT_TRUE(table.Put("abcc", "hello1").ok());
  ASSERT_TRUE(table.Put("abce", "hello1").ok());
  MemTable::Iterator it = table.GetIterator();
  it.SeekToFirst();
  ASSERT_TRUE(it.Valid());
  for (; it.Valid(); it.Next()) {
    ASSERT_EQ(it.value(), "hello1");
  }

}

}  // namespace
