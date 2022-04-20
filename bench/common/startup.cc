#include "startup.h"

#include <cstdio>
#include <unistd.h>
#include <signal.h>

namespace {

static constexpr int kReadySignal = SIGUSR1;

}  // namespace

namespace tl {
namespace bench {

void SendReadySignalToParent() {
  const pid_t parent_pid = getppid();
  if (kill(parent_pid, kReadySignal) < 0) {
    perror("SendReadySignalToParent():");
  }
}

}
}  // namespace tl
