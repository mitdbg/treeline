#include "gtest/gtest.h"

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "llsm/slice.h"
#include "util/packed_map.h"

namespace {

using llsm::Slice;
using llsm::PackedMap;

template<uint16_t Size>
class PackedMapShim {
 public:
  PackedMapShim() = default;
  PackedMapShim(const Slice& lower, const Slice& upper)
      : m_(reinterpret_cast<const uint8_t*>(lower.data()), lower.size(),
           reinterpret_cast<const uint8_t*>(upper.data()), upper.size()) {}

  bool Insert(const Slice& key, const Slice& value) {
    return m_.Insert(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                     reinterpret_cast<const uint8_t*>(value.data()),
                     value.size());
  }

  bool Remove(const Slice& key) {
    return m_.Remove(reinterpret_cast<const uint8_t*>(key.data()), key.size());
  }

  bool Get(const Slice& key, std::string* value_out) const {
    const uint8_t* payload_out = nullptr;
    unsigned payload_length = 0;
    bool found = m_.Get(reinterpret_cast<const uint8_t*>(key.data()),
                        key.size(), &payload_out, &payload_length);
    if (!found) {
      return false;
    }
    value_out->clear();
    value_out->append(reinterpret_cast<const char*>(payload_out),
                      payload_length);
    return true;
  }

 private:
  PackedMap<Size> m_;
};

TEST(PackedMapTest, SimpleReadWrite) {
  PackedMapShim<4096> map;
  const std::string key = "hello";
  const std::string value = "world";
  ASSERT_TRUE(map.Insert(key, value));

  std::string read_value;
  ASSERT_TRUE(map.Get(key, &read_value));
  ASSERT_EQ(value, read_value);
}

TEST(PackedMapTest, SimpleRemove) {
  PackedMapShim<4096> map;
  const std::string key = "hello";
  const std::string value = "world";
  ASSERT_TRUE(map.Insert(key, value));
  ASSERT_TRUE(map.Remove(key));
  ASSERT_FALSE(map.Remove(key));
  std::string read_value;
  ASSERT_FALSE(map.Get(key, &read_value));
}

TEST(PackedMapTest, BoundedReadWrite) {
  const std::string lower = "aaaaa";
  const std::string upper = "aaazz";
  PackedMapShim<4096> map(lower, upper);

  const std::string k1 = "aaabb";
  const std::string v1 = "hello world!";
  const std::string k2 = "aaacc";
  const std::string v2 = "hello world 123!";
  // Purposely insert out of order
  ASSERT_TRUE(map.Insert(k2, v2));
  ASSERT_TRUE(map.Insert(k1, v1));

  std::string v1_out, v2_out;
  ASSERT_TRUE(map.Get(k1, &v1_out));
  ASSERT_TRUE(map.Get(k2, &v2_out));
  ASSERT_EQ(v1, v1_out);
  ASSERT_EQ(v2, v2_out);
}

TEST(PackedMapTest, BoundedOverwrite) {
  const std::string lower = "aaaaa";
  const std::string upper = "aaazz";
  PackedMapShim<4096> map(lower, upper);

  const std::string k1 = "aaabb";
  const std::string k2 = "aaaff";
  const std::string k3 = "aaagg";
  const std::string v3 = "hello world 3";

  ASSERT_TRUE(map.Insert(k1, "hello world 1"));
  ASSERT_TRUE(map.Insert(k2, "hello world 2"));
  ASSERT_TRUE(map.Insert(k3, v3));

  // Re-insert k2 but with a much longer payload
  const std::string v2_new = "hello world, this is a very long payload!";
  ASSERT_TRUE(map.Insert(k2, v2_new));

  // Re-insert k1 but with a shorter payload
  const std::string v1_new = "123";
  ASSERT_TRUE(map.Insert(k1, v1_new));

  std::string v1_out, v2_out, v3_out;
  ASSERT_TRUE(map.Get(k1, &v1_out));
  ASSERT_TRUE(map.Get(k2, &v2_out));
  ASSERT_TRUE(map.Get(k3, &v3_out));
  ASSERT_EQ(v1_new, v1_out);
  ASSERT_EQ(v2_new, v2_out);
  ASSERT_EQ(v3, v3_out);
}

TEST(PackedMapTest, OverwriteCompact) {
  PackedMapShim<128> map("aa", "az");
  static_assert(sizeof(map) == 128);
  ASSERT_TRUE(map.Insert("ac", "hello"));
  ASSERT_TRUE(map.Insert("ab", "hello"));
  ASSERT_TRUE(map.Insert("ac", "he"));
  ASSERT_TRUE(map.Insert("ac", "hello123"));

  std::string ac_out, ab_out;
  ASSERT_TRUE(map.Get("ab", &ab_out));
  ASSERT_TRUE(map.Get("ac", &ac_out));
  ASSERT_EQ(ac_out, "hello123");
  ASSERT_EQ(ab_out, "hello");
}

TEST(PackedMapTest, VariableSizeInsertAndRead) {
  const std::string lower = "a", upper = "azzzzzzz";
  PackedMapShim<4096> map(lower, upper);
  std::vector<std::pair<std::string, std::string>> records = {
      {"abcd", "test"},
      {"azabdefgaaa", "much longer payload"},
      {"az", "x"},
      {"ahello", "A very very very long payload, relatively speaking."},
      {"amuchmuchlongerkey", ""}, // Length 0 payloads are allowed
      {"abcdefg", "hello"},
      {"abc", "hello world 123"},
      {"azzza", "llsm"},
      {"abcdefghijklmnopqrstuvwxyz", "abcdefghijklmnopqrstuvwxyz"},
      {"azaza", "abc"},
  };

  const Slice lower_slice(lower), upper_slice(upper);
  ASSERT_TRUE(lower_slice.compare(upper_slice) <= 0);
  for (const auto& record : records) {
    // Sanity check: make sure our keys satisfy `lower` <= `key` <= `upper. We
    // use llsm::Slice::compare() since it uses lexicographic ordering.
    ASSERT_TRUE(lower_slice.compare(record.first) <= 0);
    ASSERT_TRUE(upper_slice.compare(record.first) >= 0);
    // Now actually insert the record. The insertion should succeed.
    ASSERT_TRUE(map.Insert(record.first, record.second));
  }

  // Read the records, but not in insertion order.
  std::mt19937 rng(42);
  std::shuffle(records.begin(), records.end(), rng);
  std::string value_out;
  for (const auto& record : records) {
    ASSERT_TRUE(map.Get(record.first, &value_out));
    ASSERT_EQ(value_out, record.second);
  }
}

}  // namespace
