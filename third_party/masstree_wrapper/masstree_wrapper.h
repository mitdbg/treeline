#pragma once

#include "third_party/masstree/config.h"
// DO NOT REORDER (config.h needs to be included before other headers)
#include <optional>

#include "third_party/masstree/compiler.hh"
#include "third_party/masstree/kvthread.hh"
#include "third_party/masstree/masstree.hh"
#include "third_party/masstree/masstree_insert.hh"
#include "third_party/masstree/masstree_print.hh"
#include "third_party/masstree/masstree_remove.hh"
#include "third_party/masstree/masstree_scan.hh"
#include "third_party/masstree/masstree_stats.hh"
#include "third_party/masstree/masstree_tcursor.hh"
#include "third_party/masstree/string.hh"
#include "record_cache/record_cache_entry.h"

class key_unparse_unsigned {
 public:
  static int unparse_key(Masstree::key<uint64_t> key, char* buf, int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());
  }
};
template <typename T>
class MasstreeWrapper {
 public:
  struct table_params : public Masstree::nodeparams<15, 15> {
    using value_type = T*;
    using value_print_type = Masstree::value_print<value_type>;
    using threadinfo_type = threadinfo;
    using key_unparse_type = key_unparse_unsigned;
  };

  using Str = Masstree::Str;
  using table_type = Masstree::basic_table<table_params>;
  using unlocked_cursor_type = Masstree::unlocked_tcursor<table_params>;
  using cursor_type = Masstree::tcursor<table_params>;
  using leaf_type = Masstree::leaf<table_params>;
  using internode_type = Masstree::internode<table_params>;

  using node_type = typename table_type::node_type;
  using nodeversion_value_type =
      typename unlocked_cursor_type::nodeversion_value_type;

  static __thread typename table_params::threadinfo_type* ti;
  struct node_info_t {
    const node_type* node = nullptr;
    uint64_t old_version = 0;
    uint64_t new_version = 0;
  };

  MasstreeWrapper() { this->table_init(); }

  void table_init() {
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*ti);
  }

  static void thread_init(int thread_id) {
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  }

  bool insert_value(const char* key, std::size_t len_key, T* value) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_insert(*ti);

    if (found) {
      lp.finish(0, *ti);  // release lock
      return 0;           // not inserted
    }

    lp.value() = value;
    fence();
    lp.finish(1, *ti);  // finish insert
    return 1;           // inserted
  }

  bool insert_value_and_get_nodeinfo_on_success(const char* key,
                                                std::size_t len_key, T* value,
                                                node_info_t& node_info) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_insert(*ti);
    if (found) {
      lp.finish(0, *ti);
      return 0;
    }
    // node info is only obtained on success of insert
    node_info.node = lp.node();
    node_info.old_version = lp.previous_full_version_value();
    node_info.new_version = lp.next_full_version_value(1);
    lp.value() = value;
    fence();
    lp.finish(1, *ti);
    return 1;
  }

  T* get_value(const char* key, std::size_t len_key) {
    unlocked_cursor_type lp(table_, key, len_key);
    bool found = lp.find_unlocked(*ti);
    if (found) return lp.value();
    return nullptr;
  }

  T* get_value_and_get_nodeinfo_on_failure(const char* key, std::size_t len_key,
                                           node_info_t& node_info) {
    unlocked_cursor_type lp(table_, key, len_key);
    bool found = lp.find_unlocked(*ti);
    if (found) return lp.value();
    // node info is only obtained on failure of lookup
    node_info.node = lp.node();
    node_info.old_version = lp.full_version_value();
    node_info.new_version =
        node_info.old_version;  // version will not change with lookup
    return nullptr;
  }

  bool update_value(const char* key, std::size_t len_key, T* value) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(
        *ti);  // lock a node which potentailly contains the value

    if (found) {
      lp.value() = value;
      fence();
      assert(lp.previous_full_version_value() == lp.next_full_version_value(0));
      lp.finish(0, *ti);  // release lock
      return 1;           // updated
    }
    lp.finish(0, *ti);  // release lock
    return 0;           // not updated
  }

  bool update_value_and_get_nodeinfo_on_failure(const char* key,
                                                std::size_t len_key, T* value,
                                                node_info_t& node_info) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(
        *ti);  // lock a node which potentailly contains the value

    if (found) {
      lp.value() = value;
      fence();
      assert(lp.previous_full_version_value() == lp.next_full_version_value(0));
      lp.finish(0, *ti);  // release lock
      return 1;           // updated
    }
    // node info is only obtained on failure of lookup
    node_info.node = lp.node();
    node_info.old_version = lp.previous_full_version_value();
    node_info.new_version = lp.next_full_version_value(0);
    assert(node_info.old_version == node_info.new_version);
    lp.finish(0, *ti);  // release lock
    return 0;           // not updated
  }

  bool remove_value(const char* key, std::size_t len_key) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(
        *ti);  // lock a node which potentailly contains the value

    if (found) {
      lp.finish(-1, *ti);  // finish remove
      return 1;            // removed
    }

    assert(lp.previous_full_version_value() == lp.next_full_version_value(0));
    lp.finish(0, *ti);  // release lock
    return 0;           // not removed
  }

  bool remove_value_and_get_nodeinfo_on_failure(const char* key,
                                                std::size_t len_key,
                                                node_info_t& node_info) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(
        *ti);  // lock a node which potentailly contains the value

    if (found) {
      lp.finish(-1, *ti);  // finish remove
      return 1;            // removed
    }
    // node version is only obtained on failure of delete
    node_info.node = lp.node();
    node_info.old_version = lp.previous_full_version_value();
    node_info.new_version = lp.next_full_version_value(0);
    assert(node_info.old_version == node_info.new_version);
    lp.finish(0, *ti);  // release lock
    return 0;           // not removed
  }

  class SearchRangeScanner {
   public:
    SearchRangeScanner(
        const char* const end_key, const std::size_t end_key_length,
        bool scan_by_length, uint64_t num_records,
        std::vector<uint64_t>* indices_out,
        std::vector<tl::RecordCacheEntry>* cache_entries = nullptr,
        std::optional<uint64_t> index_locked_already = std::nullopt)
        : end_key_(end_key),
          end_key_length_(end_key_length),
          scan_by_length_(scan_by_length),
          num_records_(num_records),
          indices_out_(indices_out),
          cache_entries_(cache_entries),
          index_locked_already_(index_locked_already) {}

    template <typename ScanStackElt, typename Key>
    void visit_leaf(const ScanStackElt& iter, const Key& key, threadinfo&) {
      return;
    }

    bool visit_value(const Str key, T* val, threadinfo&) {
      if (scan_by_length_ && scanned_so_far_ >= num_records_) {
        return false;
      }

      if (scan_by_length_) {
        ++scanned_so_far_;
        lock_and_log(val);
        return true;
      }

      // compare key with end key
      const int res =
          memcmp(end_key_, key.s,
                 std::min(end_key_length_, static_cast<std::size_t>(key.len)));

      bool smaller_than_end_key = (res > 0);
      bool same_as_end_key_but_shorter =
          ((res == 0) && (end_key_length_ > static_cast<std::size_t>(key.len)));

      if (smaller_than_end_key || same_as_end_key_but_shorter) {
        lock_and_log(val);
        return true;
      }

      return false;
    }

   private:
    void lock_and_log(tl::RecordCacheEntry* val) {
      if (cache_entries_ != nullptr) {
        uint64_t index = val->FindIndexWithin(cache_entries_);
        
        if (!index_locked_already_.has_value() ||
            index_locked_already_.value() != index)
          val->Lock(/*exclusive = */ false);
        val->IncrementPriority();

        if (indices_out_ != nullptr) {
          indices_out_->emplace_back(index);
        }
      }
    }

    const char* const end_key_;
    const std::size_t end_key_length_;
    const bool scan_by_length_;
    const uint64_t num_records_;

    std::vector<uint64_t>* indices_out_;
    std::vector<tl::RecordCacheEntry>* cache_entries_;
    std::optional<uint64_t> index_locked_already_;

    uint64_t scanned_so_far_ = 0;
  };

  void scan(const char* const start_key, const std::size_t start_key_length,
            const char* const end_key, const std::size_t end_key_length,
            bool scan_by_length, uint64_t num_records,
            std::vector<uint64_t>* indices_out,
            std::vector<tl::RecordCacheEntry>* cache_entries = nullptr,
            std::optional<uint64_t> index_locked_already = std::nullopt) {
    Str mtkey =
        (start_key == nullptr ? Str() : Str(start_key, start_key_length));

    indices_out->clear();
    SearchRangeScanner scanner(end_key, end_key_length, scan_by_length,
                               num_records, indices_out, cache_entries,
                               index_locked_already);
    table_.scan(mtkey, true, scanner, *ti);
  }

  uint64_t get_version_value(const node_type* n) {
    return n->full_version_value();
  }

 private:
  table_type table_;
};

template <typename T>
__thread typename MasstreeWrapper<T>::table_params::threadinfo_type*
    MasstreeWrapper<T>::ti = nullptr;
extern volatile mrcu_epoch_type active_epoch;
extern volatile std::uint64_t globalepoch;
extern volatile bool recovering;
