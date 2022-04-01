// Acknowledgement: The code in this file was adapted from 
// https://github.com/google/cuckoo-index/blob/master/common/profiling.cc
//
// This code is licensed under the Apache 2.0 License. See
// https://github.com/google/cuckoo-index/blob/master/LICENSE

#include "timer.h"

namespace tl {

Timer& Timer::GetThreadInstance() {
  thread_local static Timer static_timer;
  return static_timer;
}

}  // namespace tl
