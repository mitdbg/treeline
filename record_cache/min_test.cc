#include <iostream>

#include "../art_olc/Tree.h"
#include "../record_cache/record_cache_entry.h"
#include "tbb/tbb.h"

llsm::RecordCacheEntry entries[10];

void loadKey(TID tid, Key& key) {
  const llsm::Slice& key_slice = entries[tid].GetKey();
  key.set(key_slice.data(), key_slice.size());
}

llsm::Slice ValueFromTID(TID tid) { return entries[tid].GetValue(); }

int main() {
  ART_OLC::Tree tree(loadKey);

  TID index = 5;

  entries[index].SetKey("aaa");
  entries[index].SetValue("bbb");

  Key key;
  loadKey(index, key);

  auto t = tree.getThreadInfo();
  tree.insert(key, index, t);

  auto val = tree.lookup(key, t);
  if (val != index) {
    std::cout << "wrong key read: " << val << " expected:" << index
              << std::endl;
    throw;
  } else {
    std::cout << "Success!" << std::endl;
  }

  return 0;
}
