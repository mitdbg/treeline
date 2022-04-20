#include "db/page.h"

#include <cstring>

#include "gtest/gtest.h"

namespace {

using namespace tl;

class PageTest : public testing::Test {
 public:
  PageTest() : buffer(new char[Page::kSize]) {
    memset(buffer.get(), 0, Page::kSize);
  }
  const std::unique_ptr<char[]> buffer;
};

TEST_F(PageTest, IterateSimple) {
  Page page(buffer.get(), "aaa", "zzz");
  const std::vector<std::pair<std::string, std::string>> records = {
      {"aab", "hello"}, {"aba", "world"}, {"zaa", "12345"}};

  // Insert records in reverse order.
  for (auto it = records.rbegin(); it != records.rend(); ++it) {
    ASSERT_TRUE(page.Put(it->first, it->second).ok());
  }

  // Iterate through the page; the records should be in ascending order.
  Page::Iterator it = page.GetIterator();
  for (const auto& record : records) {
    ASSERT_TRUE(it.Valid());
    ASSERT_TRUE(it.key().compare(record.first) == 0);
    ASSERT_TRUE(it.value().compare(record.second) == 0);
    it.Next();
  }
  ASSERT_FALSE(it.Valid());
}

TEST_F(PageTest, IteratorSeek) {
  Page page(buffer.get(), "prefix-a", "prefix-z");
  const std::vector<std::pair<std::string, std::string>> records = {
      {"prefix-g", "test0"}, {"prefix-j", "test1"}, {"prefix-x", "test2"}};

  // Insert records.
  for (const auto& record : records) {
    ASSERT_TRUE(page.Put(record.first, record.second).ok());
  }

  Page::Iterator it = page.GetIterator();

  // Should be positioned at (prefix-j, test1) after the seek.
  it.Seek("prefix-gabc");
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare(records[1].first) == 0);
  ASSERT_TRUE(it.value().compare(records[1].second) == 0);

  // Should be positioned at (prefix-g, test0) after the seek.
  it.Seek("prefix-a");
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare(records[0].first) == 0);
  ASSERT_TRUE(it.value().compare(records[0].second) == 0);

  // Should advance to the next record (prefix-j, test1).
  it.Next();
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare(records[1].first) == 0);
  ASSERT_TRUE(it.value().compare(records[1].second) == 0);

  // Should be an invalid iterator after the seek (no record after prefix-ya).
  it.Seek("prefix-ya");
  ASSERT_FALSE(it.Valid());

  // Should be positioned at (prefix-x, test2) after the seek.
  it.Seek("prefix-k");
  ASSERT_TRUE(it.Valid());
  ASSERT_TRUE(it.key().compare(records[2].first) == 0);
  ASSERT_TRUE(it.value().compare(records[2].second) == 0);

  // No remaining records.
  it.Next();
  ASSERT_FALSE(it.Valid());
}

TEST_F(PageTest, IteratorNumRecordsLeft) {
  Page page(buffer.get(), "a", "z");
  const std::vector<std::pair<std::string, std::string>> records = {
      {"b", "test0"}, {"baa", "test1"}, {"g", "test2"},
      {"j", "test3"}, {"q", "test4"},   {"x", "test5"}};

  // Insert records.
  for (const auto& record : records) {
    ASSERT_TRUE(page.Put(record.first, record.second).ok());
  }

  // Read the records out.
  Page::Iterator it = page.GetIterator();
  for (size_t i = 0; i < records.size(); ++i) {
    ASSERT_TRUE(it.Valid());
    ASSERT_TRUE(it.key().compare(records[i].first) == 0);
    ASSERT_TRUE(it.value().compare(records[i].second) == 0);
    // Records left is supposed to return the number of remaining records to
    // read (including the current record).
    ASSERT_EQ(it.RecordsLeft() + i, records.size());
    it.Next();
  }
  ASSERT_FALSE(it.Valid());
}

TEST_F(PageTest, GetKeyPrefix) {
  Page no_prefix(buffer.get());
  ASSERT_TRUE(no_prefix.GetKeyPrefix().compare("") == 0);

  Page no_prefix2(buffer.get(), "abcdefg", "z");
  ASSERT_TRUE(no_prefix2.GetKeyPrefix().compare("") == 0);

  Page long_prefix(buffer.get(), "helloaaa", "hellozzz");
  ASSERT_TRUE(long_prefix.GetKeyPrefix().compare("hello") == 0);
}

TEST_F(PageTest, UpdateOrRemove) {
 Page page(buffer.get(), "aa", "zz");
  std::string aa_out, ab_out, ac_out;

  // Simple case
  ASSERT_TRUE(page.Put("aa", "hello111").ok());
  ASSERT_TRUE(page.Get("aa", &aa_out).ok());
  ASSERT_EQ(aa_out, "hello111");

  // Fail - insert - succeed
  ASSERT_FALSE(page.UpdateOrRemove("ab", "hello222").ok());

  ASSERT_TRUE(page.Put("ab", "hello222").ok());
  ASSERT_TRUE(page.Get("ab", &ab_out).ok());
  ASSERT_EQ(ab_out, "hello222");

  ASSERT_TRUE(page.UpdateOrRemove("ab", "hello22222").ok());
  ASSERT_TRUE(page.Get("ab", &ab_out).ok());
  ASSERT_EQ(ab_out, "hello22222");

  // Fail because of payload length
  std::string s(4096, 'a');
  ASSERT_TRUE(page.UpdateOrRemove("ac", s).IsNotFound());
  ASSERT_FALSE(page.Get("ac", &ac_out).ok());
}

}  // namespace
