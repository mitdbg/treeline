#include "rand_exp_backoff.h"

namespace llsm {
namespace pg {

inline thread_local std::mt19937 RandExpBackoff::prng_{};

}  // namespace pg
}  // namespace llsm
