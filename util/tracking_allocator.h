#pragma once

#include <cassert>

// The boilerplate in this file is CC4 licenced code.
// https://howardhinnant.github.io/allocator_boilerplate.html

namespace tl {

template <class T>
class TrackingAllocator {
 public:
  using value_type = T;

  template <class U>
  struct rebind {
    typedef TrackingAllocator<U> other;
  };

  TrackingAllocator(uint64_t& currently_allocated_bytes) noexcept
      : currently_allocated_bytes_(currently_allocated_bytes) {}

  template <class U>
  TrackingAllocator(TrackingAllocator<U> const& other) noexcept
      : currently_allocated_bytes_(other.currently_allocated_bytes_) {}

  value_type* allocate(std::size_t n) {
    currently_allocated_bytes_ += n * sizeof(value_type);
    return static_cast<value_type*>(::operator new(n * sizeof(value_type)));
  }

  void deallocate(value_type* p, std::size_t n) noexcept {
    currently_allocated_bytes_ -= n * sizeof(value_type);
    ::operator delete(p);
  }

  template <class U>
  void destroy(U* p) noexcept {
    p->~U();
  }

 private:
  template <class U>
  friend class TrackingAllocator;

  uint64_t& currently_allocated_bytes_;
};

}  // namespace tl
