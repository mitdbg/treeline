#pragma once

#include <cstdint>
#include <string>
#include <thread>

#include "config.h"
#include "third_party/kvell/headers.h"
#include "util/key.h"
#include "ycsbr/ycsbr.h"

DEFINE_uint64(kvell_num_disks, 1,
              "The number of disks on which KVelldatabase files reside.");

class KVellInterface {
 public:
  KVellInterface() {}

  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}
  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {}

  // Called once before the benchmark.
  void InitializeDatabase() {
    uint64_t workers_per_disk = FLAGS_bg_threads / FLAGS_kvell_num_disks;

    slab_workers_init(FLAGS_kvell_num_disks, workers_per_disk);
  }

  // Called once after the workload if `InitializeDatabase()` has been called.
  void ShutdownDatabase() {}

  // Load the records into the database.
  //
  // This assumes an empty database initially.
  void BulkLoad(const ycsbr::BulkLoadTrace& load) {
    // Add workload item
    const char* w = "YCSB_TreeLine";
    void* workload_item = serialize_record(-10, w, strlen(w) + 1);
    struct slab_callback* cb =
        reinterpret_cast<struct slab_callback*>(malloc(sizeof(*cb)));
    cb->cb = add_in_tree;
    cb->payload = NULL;
    cb->item = workload_item;
    kv_add_async(cb);

    for (const auto& req : load) {
      if (!Insert(req.key, req.value, req.value_size)) {
        throw std::runtime_error("Failed to bulk load a record!");
      }
    }
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    struct slab_callback* cb =
        reinterpret_cast<struct slab_callback*>(malloc(sizeof(*cb)));
    cb->item = serialize_record(key, value, value_size);
    kv_update_async(cb);
    return false;
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    struct slab_callback* cb =
        reinterpret_cast<struct slab_callback*>(malloc(sizeof(*cb)));
    cb->item = serialize_record(key, value, value_size);
    kv_add_async(cb);
    return false;
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    struct slab_callback* cb =
        reinterpret_cast<struct slab_callback*>(malloc(sizeof(*cb)));
    cb->item = serialize_record(key, nullptr, 0);
    kv_read_async(cb);
    return false;
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {}

 private:
  char* serialize_record(ycsbr::Request::Key key, const char* value,
                           size_t value_size) {
    size_t meta_size = sizeof(struct item_metadata);
    char* item =
        reinterpret_cast<char*>(malloc(sizeof(key) + value_size + meta_size));
    struct item_metadata* meta = (struct item_metadata*)item;
    meta->key_size = sizeof(key);
    meta->value_size = value_size;

    auto swapped = __builtin_bswap64(key);
    memcpy(item + meta_size, &swapped, meta->key_size);
    if (value != nullptr)
      memcpy(item + meta_size + meta->key_size, value, meta->value_size);
    return item;
  }
};