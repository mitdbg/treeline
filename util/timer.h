// Acknowledgement: The code in this file was adapted from
// https://github.com/google/cuckoo-index/blob/master/common/profiling.h
//
// This code is licensed under the Apache 2.0 License. See
// https://github.com/google/cuckoo-index/blob/master/LICENSE

#pragma once

#include <chrono>
#include <cstdint>
#include <unordered_map>

namespace tl {

enum class Label {
  // Add labels here for your use case.
};

// A simple timer that can accumulate "labeled" durations. Use `ScopedTimer`
// for registering timed regions.
//
// Is thread-safe, because there can be only a single instance per thread.
class Timer {
 public:
  // Retrieves a `Timer` instance for the current thread.
  static Timer& GetThreadInstance();

  Timer(const Timer&) = delete;
  Timer& operator=(const Timer&) = delete;
  virtual ~Timer() {}

  template <typename Units>
  Units Get(Label label) const {
    const auto value_it = durations_.find(label);
    return value_it != durations_.end()
               ? std::chrono::duration_cast<Units>(value_it->second)
               : Units(0);
  }

  std::chrono::nanoseconds GetNanos(Label label) const {
    return Get<std::chrono::nanoseconds>(label);
  }

  void Reset() { durations_.clear(); }

 private:
  friend class ScopedTime;

  Timer() {}

  void Add(Label label, std::chrono::nanoseconds duration) {
    durations_[label] += duration;
  }

  std::unordered_map<Label, std::chrono::nanoseconds> durations_;
};

// Instantiate a local variable with this class to time the local scope.
// Example:
//   void MyClass::MyMethod() {
//     ScopedTime t(Label::MyClass_MyMethod);
//     .... // Do expensive stuff.
//   }
class ScopedTime {
 public:
  // ScopedTime is neither copyable nor moveable.
  ScopedTime(const Timer&) = delete;
  ScopedTime& operator=(const Timer&) = delete;

  explicit ScopedTime(Label label)
      : label_(label), start_(std::chrono::steady_clock::now()) {}

  ~ScopedTime() {
    Timer::GetThreadInstance().Add(label_,
                                   std::chrono::steady_clock::now() - start_);
  }

 private:
  Label label_;
  std::chrono::steady_clock::time_point start_;
};

}  // namespace tl
