#pragma once

#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "db/format.h"
#include "treeline/options.h"
#include "treeline/slice.h"
#include "treeline/status.h"
#include "wal/writer.h"

namespace tl {
namespace wal {

// This class provides a high-level interface to TL's write-ahead log files,
// providing methods that help with replaying the log and then writing to the
// log. The write-ahead log is segmented across multiple versioned files, so
// this manager helps provide a single logical interface over the log files.
//
// The manager has two self-explanatory modes: "Replay" and "Write". Users must
// call the `Prepare*` methods to transition the manager between modes before
// calling any of the modes' respective methods. The allowed transitions are
// depicted below (created to terminated is also a valid transition).
//
//    +--->>------------------->>---+
//    |                             |
// Created --> Replay Mode --> Write Mode --> Terminated
//                  |                             |
//                  +--->>------------------->>---+
//
// Making an unallowed transition or calling methods while in the incorrect
// "mode" are considered logic errors. The manager only performs mode checks
// using assertions, which are disabled in release mode.
//
// This class is not thread safe; it requires external mutual exclusion.
class Manager {
 public:
  // Creates a `Manager` that will read/write logs from/to `log_dir_path`. Note
  // that `log_dir_path` must be a path to a directory. If the directory does
  // not exist, it will be created.
  Manager(std::filesystem::path log_dir_path);
  ~Manager();

  Manager(const Manager&) = delete;
  Manager& operator=(const Manager&) = delete;

  // Replay mode methods

  // Must be called before any replay methods can be called. Replay can proceed
  // iff the returned status is OK.
  Status PrepareForReplay();

  using EntryCallback =
      std::function<Status(const Slice&, const Slice&, format::WriteType)>;
  // Replay the writes stored in the log.
  //
  // The callback will be called **synchronously** for each entry in the log, in
  // the order the entries were logged. The `callback` will run on this method's
  // calling thread and will be called during the execution of this method.
  //
  // The callback must return a `Status` to indicate whether or not it
  // succeeded. If the returned status is non-OK, the replay will abort early
  // and the non-OK status will be returned by this method.
  //
  // In summary, this method is semantically similar to the following
  // pseudo-code:
  // ```
  // Status ReplayLog(const EntryCallback& callback) const {
  //   for (key, value, write_type) in log {
  //     Status s = callback(key, value, write_type);
  //     if (!s.ok()) return s;
  //   }
  //   return Status::OK();
  // }
  // ```
  //
  // If the returned status is OK, then the replay was successful.
  Status ReplayLog(const EntryCallback& callback) const;

  // Write mode methods

  // Must be called before any write methods can be called. Writes can proceed
  // iff the returned status is OK. If `discard_existing_logs` is true, the
  // manager will safely remove all existing logs stored on disk.
  //
  // "Safely remove" means that the manager ensures it maintains data crash
  // consistency when removing the log files (it removes logs from oldest to
  // newest and fsyncs the log directory after each file removal).
  Status PrepareForWrite(bool discard_existing_logs = false);

  // Logs a write of `key` and `value` with type `type`. If `options.sync` is
  // true, this method will not return until the log entry has been persisted.
  Status LogWrite(const WriteOptions& options, const Slice& key,
                  const Slice& value, format::WriteType type);

  // Make the current active log version immutable and then increment the active
  // log's version. The log version before the increment will be returned.
  uint64_t IncrementLogVersion();

  // Safely removes all immutable log versions **up to and including**
  // `newest_log_version_to_discard`. Note that if the currently active log
  // version is less than `newest_log_version_to_discard`, it will not be
  // discarded.
  //
  // To ensure data consistency, this method needs to call `fsync()` on the log
  // directory after each log version is deleted. This can be a slow process.
  Status DiscardUpToInclusive(uint64_t newest_log_version_to_discard);

  // Similar to `DiscardUpToInclusive()`, except that at most one log version
  // (the oldest) will be discarded. The oldest log version will be discarded
  // iff it is less than or equal to `newest_log_version_to_discard`.
  //
  // To ensure data consistency, this method needs to call `fsync()` on the log
  // directory after the log version is deleted. This can be a slow process, but
  // does not need to be done while holding a lock. If `lock` is passed in and
  // is not null, this method will unlock the lock when it is no longer needed.
  //
  // If `lock` is not null, the lock must be held before this method is called.
  // When this method returns, the lock **will no longer be held**.
  Status DiscardOldest(uint64_t newest_log_version_to_discard,
                       std::unique_lock<std::mutex>* lock = nullptr);

  // Safely removes all immutable log versions in preparation for a clean
  // shutdown. After this method returns, the manager can no longer be used.
  // This method should only be called after all volatile data has been flushed
  // to persistent storage.
  //
  // If this method is called while the manager is **not** in "Write" mode, it
  // will be a no-op and will return an OK status. Regardless, the manager
  // should still not be used after this method returns.
  Status DiscardAllForCleanShutdown();

 private:
  // Opens the log directory (creating it if needed).
  Status OpenLogDir();
  // Scan the log directory to build the `log_versions_` deque.
  Status CollectLogVersions();
  // Convenience method that runs the two private methods above.
  Status OpenAndCollectLogVersions();

  // Returns the log path for the given `version`.
  std::filesystem::path LogPathForVersion(uint64_t version) const;

  // For internal bookkeeping.
  enum class Mode { kCreated = 0, kReplay = 1, kWrite = 2 };
  Mode mode_;

  // The file descriptor associated with the directory holding all the WAL
  // files. We keep the directory open to call `fsync()` during log creation and
  // deletion.
  int log_dir_fd_;
  const std::filesystem::path log_dir_path_;

  // Stores immutable log versions known by the manager in sorted order. The
  // front of the deque stores the oldest known version and the back of the
  // deque stores the newest version (i.e., new log versions are appended to the
  // deque).
  //
  // Log versions increase monotonically. A higher version number indicates that
  // the log is newer.
  std::deque<uint64_t> log_versions_;

  // The log version currently being written to. This value is only meaningful
  // when `mode_ == kWrite`. This value will be greater than all values in
  // `log_versions_`.
  uint64_t active_log_version_;

  // The currently active log writer. This pointer will always be null when the
  // manager is not in write mode. In write mode, this pointer will be null
  // until the first log write.
  std::unique_ptr<Writer> writer_;

  // Tracks whether or not the log directory has been fsync'd after a new log
  // file is created.
  bool log_dir_writer_synced_;
};

}  // namespace wal
}  // namespace tl
