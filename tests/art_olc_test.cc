#include <chrono>
#include <iostream>

#include "art_olc/Tree.h"
#include "gtest/gtest.h"
#include "tbb/tbb.h"

namespace {

using namespace std;

class ARTOLCTest : public testing::Test {
 public:
  ARTOLCTest() {
    keys = new uint64_t[kNumKeys];
    // Generate keys
    for (uint64_t i = 0; i < kNumKeys; i++) keys[i] = i + 1;
    tree = new ART_OLC::Tree(loadKey);
  }

  ~ARTOLCTest() { delete[] keys; }

  static void loadKey(TID tid, Key& key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific
    key.setKeyLen(sizeof(tid));
    reinterpret_cast<uint64_t*>(&key[0])[0] = __builtin_bswap64(tid);
  }

  uint64_t* keys;
  size_t kNumKeys = 10000;
  ART_OLC::Tree* tree;
};

TEST_F(ARTOLCTest, InsertLookup) {
  auto t = tree->getThreadInfo();

  for (uint64_t i = 0; i < kNumKeys; i++) {
    Key key;
    loadKey(keys[i], key);
    tree->insert(key, keys[i], t);
  }

  for (uint64_t i = 0; i < kNumKeys; i++) {
    Key key;
    loadKey(keys[i], key);
    auto val = tree->lookup(key, t);
    ASSERT_EQ(val, keys[i]);
  }
}

TEST_F(ARTOLCTest, InsertRemove) {
  auto t = tree->getThreadInfo();

  for (uint64_t i = 0; i < kNumKeys; i++) {
    Key key;
    loadKey(keys[i], key);
    tree->insert(key, keys[i], t);
  }

  for (uint64_t i = 0; i < kNumKeys; i++) {
    Key key;
    loadKey(keys[i], key);
    tree->remove(key, keys[i], t);
  }
}

TEST_F(ARTOLCTest, StartEndScan) {
  auto t = tree->getThreadInfo();

  for (uint64_t i = 0; i < kNumKeys; i++) {
    Key key;
    loadKey(keys[i], key);
    tree->insert(key, keys[i], t);
  }

  uint64_t scan_length = 100;
  for (uint64_t i = 0; i < kNumKeys - 1; i++) {
    Key start_key;
    loadKey(keys[i], start_key);

    Key end_key;
    uint64_t end_index =
        (i + scan_length < kNumKeys) ? i + scan_length : kNumKeys - 1;
    loadKey(keys[end_index], end_key);

    Key continue_key;

    TID result[scan_length];
    uint64_t found;

    auto val = tree->lookupRange(start_key, end_key, continue_key, result,
                                 scan_length, found, t);

    bool found_is_correct =
        (i < kNumKeys - scan_length) ? scan_length : kNumKeys - i;

    ASSERT_TRUE(found_is_correct);

    for (uint64_t j = 0; j < found; ++j) {
      ASSERT_EQ(result[j], keys[i + j]);
    }
  }
}

TEST_F(ARTOLCTest, StartNumberScan) {
  auto t = tree->getThreadInfo();

  for (uint64_t i = 0; i < kNumKeys; i++) {
    Key key;
    loadKey(keys[i], key);
    tree->insert(key, keys[i], t);
  }

  uint64_t scan_length = 100;
  std::vector<llsm::RecordCacheEntry> cache_entries;
  cache_entries.resize(kNumKeys);

  for (uint64_t i = 0; i < kNumKeys; i++) {
    Key start_key;
    loadKey(keys[i], start_key);

    TID result[scan_length];
    uint64_t found;

    auto val = tree->lookupRange(start_key, &cache_entries, result, scan_length,
                                 found, t);
    bool found_is_correct =
        (i < kNumKeys - scan_length) ? scan_length : kNumKeys - i;
    ASSERT_TRUE(found_is_correct);

    for (uint64_t j = 0; j < found; ++j) {
      ASSERT_EQ(result[j], keys[i + j]);
      cache_entries[result[j] - 1].Unlock();
    }
  }
}

}  // namespace
