#include "rand_exp_backoff.h"

namespace tl {
namespace pg {

inline thread_local std::mt19937 RandExpBackoff::prng_{};

}  // namespace pg
}  // namespace tl
