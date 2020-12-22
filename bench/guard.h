#pragma once

#include <functional>

namespace llsm {
namespace bench {

class CallOnExit {
 public:
  CallOnExit(std::function<void(void)> fn) : fn_(std::move(fn)) {}
  ~CallOnExit() { fn_(); }
 private:
  std::function<void(void)> fn_;
};

}  // namespace bench
}  // namespace llsm
