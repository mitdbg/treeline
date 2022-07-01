#pragma once

#include <mutex>
#include <unordered_map>

namespace tl {

// A FIFO queue that supports fast random-item removal.
//
// This class implements a doubly-linked list that supports (amortized)
// constant-time removal of random items using an std::unordered_map
template <class T>
class HashQueue {
  // A node of the HashQueue.
  template <class U>
  struct HashQueueNode {
    struct HashQueueNode<U>* prev;
    U item;
    struct HashQueueNode<U>* next;
  };

 public:
  // The default minimum number of elements based on which the buckets are set.
  static const size_t kDefaultMinNumElements = 1024;

  // Construct a new HashQueue of type T, optionally reserving enough space for
  // the buckets needed to efficiently access `num_elements`.
  HashQueue<T>() : HashQueue<T>::HashQueue(kDefaultMinNumElements) {}
  HashQueue<T>(size_t num_elements) {
    item_to_node_map_.reserve(num_elements);
    front_ = nullptr;
    back_ = nullptr;
  }

  // Free all resources.
  ~HashQueue() {
    // Delete space for nodes.
    for (auto pair : item_to_node_map_) {
      if (pair.second != nullptr) {
        delete pair.second;
      }
    }
  }

  // Return whether the HashQueue is currently empty.
  // DEPRECATED, NOT THREAD SAFE
  // (left for compatibility with old LRU buffer manager code)
  bool IsEmpty() { return (front_ == nullptr); }

  // Insert `item` into the HashQueue (at the back of the queue and in the
  // unordered_map).
  void Enqueue(T item) {
    // Create a new node
    struct HashQueueNode<T>* new_node = new struct HashQueueNode<T>;

    new_node->item = item;
    new_node->next = nullptr;
    mu_.lock();
    new_node->prev = back_;

    // Enqueue into linked list
    if (back_ != nullptr) {
      back_->next = new_node;
    } else {
      front_ = new_node;
    }
    back_ = new_node;

    // Insert into unordered_map
    item_to_node_map_.insert({item, new_node});
    mu_.unlock();
  }

  // Remove and return the next item from the HashQueue (at the front of the
  // queue).
  T Dequeue() {
    // Dequeue from linked list
    mu_.lock();

    if (front_ == nullptr) {
      // Corner case: empty data structure
      mu_.unlock();
      return 0;
    }

    struct HashQueueNode<T>* old_front = front_;
    front_ = old_front->next;
    T item = old_front->item;
    if (front_ != nullptr) {
      front_->prev = nullptr;
    } else {
      back_ = nullptr;
    }

    // Remove from unordered map
    item_to_node_map_.erase(item);
    delete old_front;

    mu_.unlock();

    return item;
  }

  // Return whether or not `item` is currently in the HashQueue.
  bool Contains(T item) {
    mu_.lock();
    bool found = item_to_node_map_.contains(item);
    mu_.unlock();
    return found;
  }

  // Delete `item` from the HashQueue, if it is there. Returns true for
  // successful deletion, false if the item was not found.
  bool Delete(T item) {
    // Retrieve node
    mu_.lock();
    auto to_delete_lookup = item_to_node_map_.find(item);
    if (to_delete_lookup == item_to_node_map_.end()) {
      mu_.unlock();
      return false;
    }
    struct HashQueueNode<T>* to_delete = to_delete_lookup->second;

    // Remove from linked list
    if (to_delete->prev != nullptr) {
      to_delete->prev->next = to_delete->next;
    } else {
      front_ = to_delete->next;
    }
    if (to_delete->next != nullptr) {
      to_delete->next->prev = to_delete->prev;
    } else {
      back_ = to_delete->prev;
    }

    // Remove from unordered map
    item_to_node_map_.erase(item);
    delete to_delete;
    mu_.unlock();

    return true;
  }

  // Moves an item to the back of the queue if it is already in the HashQueue,
  // or enqueues it otherwise. Returns true iff the item was already present in
  // the HashQueue.
  bool MoveToBack(T item) {
    // Retrieve node
    mu_.lock();
    auto to_move_lookup = item_to_node_map_.find(item);
    if (to_move_lookup == item_to_node_map_.end()) {
      mu_.unlock();
      Enqueue(item);
      return false;
    }
    struct HashQueueNode<T>* to_move = to_move_lookup->second;

    // Remove node from its current position in the linked list.
    if (to_move->prev != nullptr) {
      to_move->prev->next = to_move->next;
    } else {
      front_ = to_move->next;
    }
    if (to_move->next != nullptr) {
      to_move->next->prev = to_move->prev;
    } else {
      back_ = to_move->prev;
    }

    to_move->next = nullptr;
    to_move->prev = back_;

    // Enqueue into the back of the linked list.
    if (back_ != nullptr) {
      back_->next = to_move;
    } else {
      front_ = to_move;
    }
    back_ = to_move;

    mu_.unlock();

    return true;
  }

  class Iterator;
  Iterator begin() { return Iterator(front_); }
  Iterator end() { return Iterator(nullptr); }

  class Iterator {
   public:
    Iterator() : curr_node_(front_) {}
    Iterator(const HashQueueNode<T>* node) : curr_node_(node) {}

    Iterator& operator=(HashQueueNode<T>* node) {
      this->curr_node_ = node;
      return *this;
    }

    // Prefix ++ overload
    Iterator& operator++() {
      if (curr_node_) curr_node_ = curr_node_->next;
      return *this;
    }

    // Postfix ++ overload
    Iterator operator++(int) {
      Iterator iterator = *this;
      ++*this;
      return iterator;
    }

    bool operator!=(const Iterator& iterator) {
      return curr_node_ != iterator.curr_node_;
    }

    T operator*() { return curr_node_->item; }

   private:
    const HashQueueNode<T>* curr_node_;
  };

 private:
  // An unordered map from items to HashQueueNode's
  std::unordered_map<T, struct HashQueueNode<T>*> item_to_node_map_;

  // The front of the queue
  struct HashQueueNode<T>* front_;

  // The back of the queue
  struct HashQueueNode<T>* back_;

  // A mutex for concurrency control.
  std::mutex mu_;
};

}  // namespace tl
