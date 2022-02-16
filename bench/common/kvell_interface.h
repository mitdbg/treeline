#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

#include "config.h"
//#include "third_party/kvell/headers.h"
#include "util/key.h"
#include "ycsbr/ycsbr.h"

class KVellInterface {
 public:
  KVellInterface() /*: db_(nullptr)*/ {}

  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}
  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {}
  void WriteOutStats(const std::filesystem::path& out_dir) {}

  // Called once before the benchmark.
  void InitializeDatabase() {}

  // Called once after the workload if `InitializeDatabase()` has been called.
  void ShutdownDatabase() {
    /* if (db_ == nullptr) {
       return;
     }
     delete db_;
     db_ = nullptr; */
  }

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& load) {}

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    /*  struct slab_callback* cb = malloc(sizeof(*cb));
      cb->item = create_unique_item(key, value, value_size);
      kv_update_async(cb);*/
    return false;
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    /*  struct slab_callback* cb = malloc(sizeof(*cb));
      cb->item = create_unique_item(key, value, value_size);
      kv_add_async(cb); */
    return false;
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) { return false; }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    return false;
  }

 private:
  /*char* create_unique_item(ycsbr::Request::Key key, const char* value,
                           size_t value_size) {
    size_t meta_size = sizeof(struct item_metadata);
    char* item = malloc(sizeof(key) + value_size + meta_size);
    struct item_metadata* meta = (struct item_metadata*)item;
    meta->key_size = sizeof(key);
    meta->value_size = value_size;

    auto swapped = __builtin_bswap64(key);
    memcpy(item + meta_size, &swapped, meta->key_size);
    memcpy(item + meta_size + meta->key_size, value, meta->value_size);
    return item;
  }*/
};
