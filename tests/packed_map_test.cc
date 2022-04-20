#include "util/packed_map.h"

#include <algorithm>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "treeline/slice.h"

namespace {

using tl::PackedMap;
using tl::Slice;

template <uint16_t Size>
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

  bool UpdateOrRemove(const Slice& key, const Slice& value) {
    return m_.UpdateOrRemove(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                     reinterpret_cast<const uint8_t*>(value.data()),
                     value.size());
  }

  bool Append(const Slice& key, const Slice& value, const bool perform_checks) {
    return m_.Append(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                     reinterpret_cast<const uint8_t*>(value.data()),
                     value.size(), perform_checks);
  }

  int8_t CanAppend(const Slice& key) {
    return m_.CanAppend(reinterpret_cast<const uint8_t*>(key.data()),
                        key.size());
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

  uint16_t GetNumRecords() const { return m_.GetNumRecords(); }

  Slice GetKeyPrefix() const {
    const uint8_t* prefix = nullptr;
    unsigned length = 0;
    m_.GetKeyPrefix(&prefix, &length);
    return Slice(reinterpret_cast<const char*>(prefix), length);
  }

  std::optional<Slice> GetKeySuffixInSlot(uint16_t slot_id) const {
    const uint8_t* suffix = nullptr;
    unsigned length = 0;
    if (!m_.GetKeySuffixInSlot(slot_id, &suffix, &length)) {
      return std::optional<Slice>();
    }
    return Slice(reinterpret_cast<const char*>(suffix), length);
  }

  std::optional<Slice> GetPayloadInSlot(uint16_t slot_id) const {
    const uint8_t* payload = nullptr;
    unsigned length = 0;
    if (!m_.GetPayloadInSlot(slot_id, &payload, &length)) {
      return std::optional<Slice>();
    }
    return Slice(reinterpret_cast<const char*>(payload), length);
  }

  uint16_t LowerBoundSlot(const Slice& key) const {
    return m_.LowerBoundSlot(reinterpret_cast<const uint8_t*>(key.data()),
                             key.size());
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
  ASSERT_FALSE(map.Remove(key));  // Will be no-op
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
  ASSERT_TRUE(map.Insert("ac", "he"));
  ASSERT_TRUE(map.Insert("ab", "he"));
  ASSERT_TRUE(map.Insert("ac", "h"));
  ASSERT_TRUE(map.Insert("ac", "he"));

  std::string ac_out, ab_out;
  ASSERT_TRUE(map.Get("ab", &ab_out));
  ASSERT_TRUE(map.Get("ac", &ac_out));
  ASSERT_EQ(ac_out, "he");
  ASSERT_EQ(ab_out, "he");
}

TEST(PackedMapTest, VariableSizeInsertAndRead) {
  const std::string lower = "a", upper = "azzzzzzz";
  PackedMapShim<4096> map(lower, upper);
  std::vector<std::pair<std::string, std::string>> records = {
      {"abcd", "test"},
      {"azabdefgaaa", "much longer payload"},
      {"az", "x"},
      {"ahello", "A very very very long payload, relatively speaking."},
      {"amuchmuchlongerkey", ""},  // Length 0 payloads are allowed
      {"abcdefg", "hello"},
      {"abc", "hello world 123"},
      {"azzza", "tl"},
      {"abcdefghijklmnopqrstuvwxyz", "abcdefghijklmnopqrstuvwxyz"},
      {"azaza", "abc"},
  };

  const Slice lower_slice(lower), upper_slice(upper);
  ASSERT_TRUE(lower_slice.compare(upper_slice) <= 0);
  for (const auto& record : records) {
    // Sanity check: make sure our keys satisfy `lower` <= `key` <= `upper. We
    // use tl::Slice::compare() since it uses lexicographic ordering.
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

TEST(PackedMapTest, AppendRead) {
  PackedMapShim<4096> map("aa", "az");
  std::string ab_out, ac_out, ae_out, af_out, ag_out;

  // Simple case
  ASSERT_TRUE(map.Append("aa", "hello111", false));
  ASSERT_TRUE(map.Append("ab", "hello222", false));
  ASSERT_TRUE(map.Append("ad", "hello444", false));
  ASSERT_TRUE(map.Append("ae", "hello555", false));

  ASSERT_TRUE(map.Get("ab", &ab_out));
  ASSERT_EQ(ab_out, "hello222");

  // Try to append smaller key (should succeed but use Insert() under the hood).
  ASSERT_TRUE(map.Append("ac", "hello333", true));
  ASSERT_TRUE(map.Get("ac", &ac_out));

  // Try to change payload of top key
  ASSERT_TRUE(map.Append("ae", "hello000", true));  // equal length
  ASSERT_TRUE(map.Get("ae", &ae_out));
  ASSERT_EQ(ae_out, "hello000");

  ASSERT_TRUE(map.Append("ae", "hello55", true));  // shorter
  ASSERT_TRUE(map.Get("ae", &ae_out));
  ASSERT_EQ(ae_out, "hello55");

  ASSERT_TRUE(map.Append("ae", "hello5555", true));  // longer
  ASSERT_TRUE(map.Get("ae", &ae_out));
  ASSERT_EQ(ae_out, "hello5555");

  // Mix insert/append
  ASSERT_TRUE(map.Append("ag", "hello777", false));
  ASSERT_TRUE(map.Insert("af", "hello666"));
  ASSERT_TRUE(map.Append("ai", "hello999", false));
  ASSERT_TRUE(map.Insert("ah", "hello888"));

  ASSERT_TRUE(map.Get("af", &af_out));
  ASSERT_TRUE(map.Get("ag", &ag_out));
  ASSERT_EQ(af_out, "hello666");
  ASSERT_EQ(ag_out, "hello777");
}

TEST(PackedMapTest, CanAppend) {
  PackedMapShim<4096> map("aa", "az");

  // Can append when empty
  ASSERT_EQ(map.CanAppend("aa"), (int8_t)1);

  ASSERT_TRUE(map.Append("aa", "hello111", false));
  ASSERT_TRUE(map.Append("ac", "hello333", false));

  // Can't append smaller
  ASSERT_EQ(map.CanAppend("ab"), (int8_t)-1);

  // Can append larger
  ASSERT_EQ(map.CanAppend("ad"), (int8_t)1);
  ASSERT_TRUE(map.Append("ad", "hello444", false));

  // Can update largest
  ASSERT_EQ(map.CanAppend("ad"), (int8_t)0);
}

TEST(PackedMapTest, GetNumRecords) {
  PackedMapShim<4096> map("aa", "az");
  ASSERT_EQ(map.GetNumRecords(), 0);

  ASSERT_TRUE(map.Insert("ab", "hello"));
  ASSERT_EQ(map.GetNumRecords(), 1);

  // Same key - the existing record should be replaced.
  ASSERT_TRUE(map.Insert("ab", "hello123"));
  ASSERT_EQ(map.GetNumRecords(), 1);

  ASSERT_TRUE(map.Insert("ar", "hello world!"));
  ASSERT_EQ(map.GetNumRecords(), 2);
}

TEST(PackedMapTest, GetKeyPrefix) {
  PackedMapShim<4096> empty_prefix;
  ASSERT_TRUE(empty_prefix.GetKeyPrefix().compare("") == 0);

  PackedMapShim<4096> hello_prefix("helloabc", "helloworld");
  ASSERT_TRUE(hello_prefix.GetKeyPrefix().compare("hello") == 0);

  PackedMapShim<65528> char_prefix("abcdefg", "az");
  ASSERT_TRUE(char_prefix.GetKeyPrefix().compare("a") == 0);
}

TEST(PackedMapTest, GetKeySuffixInSlot) {
  PackedMapShim<4096> empty_prefix;
  ASSERT_TRUE(empty_prefix.Insert("hello", "world"));
  ASSERT_TRUE(empty_prefix.GetKeySuffixInSlot(0)->compare("hello") == 0);
  ASSERT_FALSE(empty_prefix.GetKeySuffixInSlot(1).has_value());

  PackedMapShim<4096> hello_prefix("helloabc", "helloworld");
  ASSERT_TRUE(hello_prefix.Insert("helloaaa", "world"));
  ASSERT_TRUE(hello_prefix.Insert("helloccc", "world"));
  ASSERT_TRUE(hello_prefix.GetKeySuffixInSlot(0)->compare("aaa") == 0);
  ASSERT_TRUE(hello_prefix.GetKeySuffixInSlot(1)->compare("ccc") == 0);
  ASSERT_FALSE(hello_prefix.GetKeySuffixInSlot(2).has_value());
}

TEST(PackedMapTest, GetPayloadInSlot) {
  PackedMapShim<4096> empty_prefix;
  ASSERT_TRUE(empty_prefix.Insert("hello", "world"));
  ASSERT_TRUE(empty_prefix.GetPayloadInSlot(0)->compare("world") == 0);
  ASSERT_FALSE(empty_prefix.GetPayloadInSlot(1).has_value());

  PackedMapShim<4096> hello_prefix("helloabc", "helloworld");
  ASSERT_TRUE(hello_prefix.Insert("helloaaa", "world"));
  ASSERT_TRUE(hello_prefix.Insert("helloccc", "worldccc"));
  ASSERT_TRUE(hello_prefix.GetPayloadInSlot(0)->compare("world") == 0);
  ASSERT_TRUE(hello_prefix.GetPayloadInSlot(1)->compare("worldccc") == 0);
  ASSERT_FALSE(hello_prefix.GetPayloadInSlot(2).has_value());
}

TEST(PackedMapTest, LowerBoundSlot) {
  PackedMapShim<4096> empty_prefix;
  ASSERT_EQ(empty_prefix.LowerBoundSlot("hello"), 0);

  ASSERT_TRUE(empty_prefix.Insert("hello", "world"));
  ASSERT_EQ(empty_prefix.LowerBoundSlot("hello"), 0);
  ASSERT_EQ(empty_prefix.LowerBoundSlot("hellozzz"), 1);

  ASSERT_TRUE(empty_prefix.Insert("zzz", "123"));
  ASSERT_EQ(empty_prefix.LowerBoundSlot("abc"), 0);
  ASSERT_EQ(empty_prefix.LowerBoundSlot("zzz123"), 2);
  ASSERT_EQ(empty_prefix.LowerBoundSlot("helloa"), 1);

  PackedMapShim<4096> hello_prefix("helloabc", "helloworld");
  ASSERT_TRUE(hello_prefix.Insert("helloaaa", "world"));
  ASSERT_TRUE(hello_prefix.Insert("helloccc", "worldccc"));

  ASSERT_EQ(hello_prefix.LowerBoundSlot("helloa"), 0);
  ASSERT_EQ(hello_prefix.LowerBoundSlot("helloaaabbb"), 1);
  ASSERT_EQ(hello_prefix.LowerBoundSlot("helloccc"), 1);
  ASSERT_EQ(hello_prefix.LowerBoundSlot("hellocccd"), 2);
}

TEST(PackedMapTest, UpdateOrRemove) {
  PackedMapShim<4096> map("aa", "az");
  std::string aa_out, ab_out, ac_out;

  // Simple case
  ASSERT_TRUE(map.Insert("aa", "hello111"));
  ASSERT_TRUE(map.Get("aa", &aa_out));
  ASSERT_EQ(aa_out, "hello111");

  // Fail - insert - succeed
  ASSERT_FALSE(map.UpdateOrRemove("ab", "hello222"));

  ASSERT_TRUE(map.Insert("ab", "hello222"));
  ASSERT_TRUE(map.Get("ab", &ab_out));
  ASSERT_EQ(ab_out, "hello222");

  ASSERT_TRUE(map.UpdateOrRemove("ab", "hello22222"));
  ASSERT_TRUE(map.Get("ab", &ab_out));
  ASSERT_EQ(ab_out, "hello22222");

  // Fail - append - succeed
  ASSERT_FALSE(map.UpdateOrRemove("ac", "hello333"));

  ASSERT_TRUE(map.Append("ac", "hello333", true));
  ASSERT_TRUE(map.Get("ac", &ac_out));
  ASSERT_EQ(ac_out, "hello333");

  ASSERT_TRUE(map.UpdateOrRemove("ac", "hello33333"));
  ASSERT_TRUE(map.Get("ac", &ac_out));
  ASSERT_EQ(ac_out, "hello33333");

  // Fail because of payload length
  std::string s(4096, 'a');
  ASSERT_FALSE(map.UpdateOrRemove("ac", s));
  ASSERT_FALSE(map.Get("ac", &ac_out));
}

}  // namespace
