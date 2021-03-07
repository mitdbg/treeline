#pragma once

#include <functional>
#include <shared_mutex>
#include <vector>
#include <iostream>

namespace llsm {

// A hash table with separate chaining that allows concurrent operations to
// different buckets.

template <class KeyType, class ValueType>
class SyncHashTable {
  // A node of the hash table
  struct Node {
    Node* prev = nullptr;
    KeyType key;
    ValueType value;
    Node* next = nullptr;
  };

 public:
  SyncHashTable(size_t capacity = 10, size_t num_partitions = 2,
                double max_load_factor = 1.0) {
    bucket_count_ = capacity / max_load_factor;
    num_partitions_ = num_partitions;

    for (size_t i = 0; i < bucket_count_; ++i) buckets_.push_back(nullptr);

    for (size_t i = 0; i < num_partitions_; ++i)
      mutexes_.emplace_back(new std::shared_mutex);
  }

  ~SyncHashTable() {
    for (const auto& mut : mutexes_) {
      delete mut;
    }

    for (const auto& bucket : buckets_) {
      Node* current = bucket;
      while (current != nullptr) {
        Node* next = current->next;
        delete current;
        current = next;
      }
    }
  }

  // Insert the pair (`key`, `value`) into the container, replacing any previous
  // value associated with `key`, if any, without touching any of the mutexes.
  // Returns true if `key` was already present in the container, and false
  // otherwise.
  bool UnsafeInsert(KeyType key, ValueType value) {
    size_t bucket = Bucket(key);

    Node* current = buckets_.at(bucket);

    // Corner case: empty bucket
    if (current == nullptr) {
      Node* new_node = new Node;
      new_node->key = key;
      new_node->value = value;
      buckets_.at(bucket) = new_node;
      return false;
    }

    // Otherwise look for it
    while (current != nullptr) {
      if (current->key == key) {
        current->value = value;
        return true;
      } else {
        if (current->next == nullptr) break;
        current = current->next;
      }
    }

    Node* new_node = new Node;
    new_node->key = key;
    new_node->value = value;
    new_node->prev = current;
    current->next = new_node;
    return false;
  }

  // Insert the pair (`key`, `value`) into the container, replacing any previous
  // value associated with `key`, if any, while exclusively holding the
  // corresponding mutex. Returns true if `key` was already present in the
  // container, and false otherwise.
  bool SafeInsert(KeyType key, ValueType value) {
    LockMutexByKey(key, /*exclusive = */ true);
    bool found = UnsafeInsert(key, value);
    UnlockMutexByKey(key, /*exclusive = */ true);
    return found;
  }

  // Erase `key` and any associated value from the container, if it was present,
  // without touching any of the mutexes. Returns true if `key` was indeed
  // present in the container, and false otherwise.
  bool UnsafeErase(KeyType key) {
    size_t bucket = Bucket(key);

    Node* current = buckets_.at(bucket);
    while (current != nullptr) {
      if (current->key == key) {
        RemoveNode(current, bucket);
        return true;
      } else {
        current = current->next;
      }
    }
    return false;
  }

  // Erase `key` and any associated value from the container, if it was present,
  // while exclusively holding the corresponding mutex. Returns true if `key`
  // was indeed present in the container, and false otherwise.
  bool SafeErase(KeyType key) {
    LockMutexByKey(key, /*exclusive = */ true);
    bool found = UnsafeErase(key);
    UnlockMutexByKey(key, /*exclusive = */ true);
    return found;
  }

  // Look up `key` in the container, without touching any of the mutexes.
  // Returns true if `key` was indeed present in the container, and false
  // otherwise. If returning true, *`value_out` is also set to the value
  // associated with `key` in the container.
  bool UnsafeLookup(KeyType key, ValueType* value_out) {
    size_t bucket = Bucket(key);

    Node* current = buckets_.at(bucket);
    while (current != nullptr) {
      if (current->key == key) {
        *value_out = current->value;
        return true;
      } else {
        current = current->next;
      }
    }
    return false;
  }

  // Look up `key` in the container, while holding the corresponding mutex in
  // shared mode. Returns true if `key` was indeed present in the container, and
  // false otherwise. If returning true, *`value_out` is also set to the value
  // associated with `key` in the container.
  bool SafeLookup(KeyType key, ValueType* value_out) {
    LockMutexByKey(key, /*exclusive  = */ false);
    bool found = UnsafeLookup(key, value_out);
    UnlockMutexByKey(key, /*exclusive  = */ false);
    return found;
  }

  // Return the bucket of `key`.
  size_t Bucket(KeyType key) const { return HashKey(key) % bucket_count_; }

  // Return the id of the mutex of `bucket`.
  size_t MutexId(size_t bucket) const { return bucket % num_partitions_; }

  // Lock/unlock the mutex associated with the `bucket` of `key`, which is
  // either known explicitly (..ByBucket) or calculated (.. ByKey).
  void LockMutexByKey(const KeyType key, bool exclusive) {
    const size_t bucket = Bucket(key);
    const size_t mutex_id = MutexId(bucket);
    LockMutexById(mutex_id, exclusive);
  }
  void LockMutexById(const size_t mutex_id, bool exclusive) {
    exclusive ? mutexes_.at(mutex_id)->lock()
              : mutexes_.at(mutex_id)->lock_shared();
  }
  void UnlockMutexByKey(const KeyType key, bool exclusive) {
    const size_t bucket = Bucket(key);
    const size_t mutex_id = MutexId(bucket);
    UnlockMutexById(mutex_id, exclusive);
  }
  void UnlockMutexById(const size_t mutex_id, bool exclusive) {
    exclusive ? mutexes_.at(mutex_id)->unlock()
              : mutexes_.at(mutex_id)->unlock_shared();
  }

  // Serializes the simultaneous locking of two members of map_mutex_, by always
  // locking the lower-indexed mutex first
  void JointlyLockMutexes(const KeyType old_key, const KeyType new_key,
                          bool old_is_valid, bool exclusive) {
    const size_t old_mutex_id = MutexId(Bucket(old_key));
    const size_t new_mutex_id = MutexId(Bucket(new_key));

    if (old_mutex_id == new_mutex_id || !old_is_valid) {
      LockMutexById(new_mutex_id, exclusive);
    } else if (new_mutex_id > old_mutex_id) {
      LockMutexById(old_mutex_id, exclusive);
      LockMutexById(new_mutex_id, exclusive);
    } else {
      LockMutexById(new_mutex_id, exclusive);
      LockMutexById(old_mutex_id, exclusive);
    }
  }

  // When holding two simultaneous members of map_mutex_ in FixPage(),
  // unlocks the mutex associated with the old (evicted) page_id iff
  // it is distinct from the mutex associated with the new page_id.
  void UnlockOldMutexIfPossible(const KeyType old_key, const KeyType new_key,
                                bool old_is_valid, bool exclusive) {
    if (!old_is_valid) return;
    const size_t old_mutex_id = MutexId(Bucket(old_key));
    const size_t new_mutex_id = MutexId(Bucket(new_key));
    if (old_mutex_id != new_mutex_id) UnlockMutexById(old_mutex_id, exclusive);
  }

 private:
  // Remove `current` from the container and delete it, updating the pointers of
  // its neighbors in `bucket` appropriately.
  void RemoveNode(Node* current, size_t bucket) {
    if (current->prev != nullptr) {
      current->prev->next = current->next;
    } else {
      buckets_.at(bucket) = current->next;
    }
    if (current->next != nullptr) {
      current->next->prev = current->prev;
    }
    delete current;
  }

  // Return the hash of `key`.
  size_t HashKey(KeyType key) const { return std::hash<KeyType>{}(key); }

  std::vector<Node*> buckets_;

  std::vector<std::shared_mutex*> mutexes_;

  size_t bucket_count_;
  size_t num_partitions_;
};

}  // namespace llsm
