#pragma once

namespace tl {
namespace bench {

// Called by the benchmark driver to notify its parent process when it has
// finished initializing the database and will start running the workload.
// This function will send the parent process a `SIGUSR1` signal.
//
// The parent process can use this signal to know when to start making
// "physical" measurements (e.g., physical I/O while the workload runs).
void SendReadySignalToParent();

}  // namespace bench
}  // namespace tl
