#include "manager.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <limits>
#include <system_error>

#include "util/coding.h"
#include "wal/reader.h"

namespace fs = std::filesystem;

namespace {

// Attempts to parse `filename` as if it were an integer. If the parse is
// successful, this method will return true and the parsed value will be
// returned via `version_out`. Otherwise this method returns false.
bool ParseLogVersion(const std::string& filename, uint64_t* version_out) {
  try {
    *version_out = std::stoull(filename, nullptr, /*base=*/10);
    return true;
  } catch (const std::invalid_argument& ex) {
    return false;
  } catch (const std::out_of_range& ex) {
    return false;
  }
}

}  // namespace

namespace tl {
namespace wal {

Manager::Manager(fs::path log_dir_path)
    : mode_(Mode::kCreated),
      log_dir_fd_(-1),
      log_dir_path_(std::move(log_dir_path)),
      log_versions_(),
      active_log_version_(0),
      writer_(nullptr),
      log_dir_writer_synced_(false) {}

Manager::~Manager() {
  if (mode_ == Mode::kCreated) {
    return;
  }
  close(log_dir_fd_);
}

Status Manager::PrepareForReplay() {
  assert(mode_ == Mode::kCreated);

  // Handle the kCreated -> kReplay transition.
  Status status = OpenAndCollectLogVersions();
  if (!status.ok()) return status;
  mode_ = Mode::kReplay;
  return status;
}

Status Manager::ReplayLog(const EntryCallback& callback) const {
  assert(mode_ == Mode::kReplay);

  std::string scratch;
  Slice entry;

  // Replay the log files in order from oldest to newest.
  for (uint64_t version : log_versions_) {
    Reader reader(LogPathForVersion(version),
                  /*reporter=*/nullptr, /*checksum=*/true,
                  /*initial_offset=*/0);
    // If there was a problem opening this log file, abort and report the error.
    const Status reader_creation_status = reader.GetCreationStatus();
    if (!reader_creation_status.ok()) return reader_creation_status;

    // Read all the log entries. `Reader::ReadEntry()` returns false when there
    // are no more entries in the particular log file.
    while (reader.ReadEntry(&entry, &scratch)) {
      // See LogWrite() for the serialization format.
      assert(entry.size() > 0);

      // Extract the write type, skipping if unrecognized.
      const uint8_t raw_type = entry[0];
      entry.remove_prefix(1);
      if (raw_type > format::kMaxWriteType) continue;
      const format::WriteType type = static_cast<format::WriteType>(raw_type);

      // Extract the encoded key length.
      uint32_t key_length = 0;
      if (!GetVarint32(&entry, &key_length)) continue;

      // GetVarint32() "removes" `key_length` from the slice.
      // If the entry becomes too short, skip it.
      if (entry.size() < key_length) continue;

      // Key and value are stored contiguously.
      Slice key(entry.data(), key_length);
      Slice value(entry.data() + key_length, entry.size() - key_length);

      // Pass the record to the caller.
      const Status s = callback(key, value, type);
      if (!s.ok()) return s;
    }
  }
  return Status::OK();
}

Status Manager::PrepareForWrite(bool discard_existing_logs) {
  assert(mode_ == Mode::kCreated || mode_ == Mode::kReplay);

  if (mode_ == Mode::kCreated) {
    // Ensure the log versions deque has been initialized.
    Status status = OpenAndCollectLogVersions();
    if (!status.ok()) return status;
  }

  if (discard_existing_logs && !log_versions_.empty()) {
    Status status = DiscardUpToInclusive(log_versions_.back());
    if (!status.ok()) return status;
    assert(log_versions_.empty());
  }

  if (!log_versions_.empty()) {
    active_log_version_ = log_versions_.back() + 1;
  } else {
    active_log_version_ = 0;
  }

  mode_ = Mode::kWrite;
  log_dir_writer_synced_ = false;
  return Status::OK();
}

Status Manager::LogWrite(const WriteOptions& options, const Slice& key,
                         const Slice& value, format::WriteType type) {
  assert(mode_ == Mode::kWrite);

  // The writer is lazily initialized so that we avoid creating any log files if
  // the WAL was bypassed for all records in a memtable generation.
  if (writer_ == nullptr) {
    writer_.reset(new Writer(LogPathForVersion(active_log_version_)));
    Status status = writer_->GetCreationStatus();
    if (!status.ok()) return status;
  }

  // Record serialization format:
  // - Write type (1 byte)
  // - Key length (varint32)
  // - Key bytes
  // - Value bytes
  // We do not need to encode the value size because the WAL already encodes the
  // total serialized record size.
  //
  // NOTE: This encoding assumes key lengths are at most 2^32 - 1. TL
  // currently cannot store keys larger than the on-disk page size (64 KiB), so
  // this is a valid assumption.
  assert(key.size() <= std::numeric_limits<uint32_t>::max());

  std::string serialized_record;
  // In the worst case the key length takes up 5 bytes. However most keys are
  // short enough for their lengths to be encoded using 1 byte.
  serialized_record.reserve(1 + 1 + key.size() + value.size());
  serialized_record.push_back(static_cast<uint8_t>(type));
  PutVarint32(&serialized_record, key.size());
  serialized_record.append(key.data(), key.size());
  serialized_record.append(value.data(), value.size());

  Status status = writer_->AddEntry(options, Slice(serialized_record));
  if (!status.ok() || !options.sync || log_dir_writer_synced_) {
    return status;
  }

  // Call fsync() on the log directory to be sure that the log file entry has
  // been persisted.
  if (fsync(log_dir_fd_) != 0) {
    return Status::FromPosixError(log_dir_path_.string(), errno);
  }
  log_dir_writer_synced_ = true;
  return Status::OK();
}

uint64_t Manager::IncrementLogVersion() {
  assert(mode_ == Mode::kWrite);
  log_versions_.push_back(active_log_version_);
  ++active_log_version_;
  log_dir_writer_synced_ = false;
  writer_.reset();
  return log_versions_.back();
}

Status Manager::DiscardUpToInclusive(uint64_t newest_log_version_to_discard) {
  // This method is only meant to be publicly called when in write mode.
  // However, we use it internally in different modes for preparation too. So we
  // do not assert `mode_ == Mode::kWrite` here.
  try {
    while (!log_versions_.empty()) {
      const uint64_t oldest_version = log_versions_.front();
      if (oldest_version > newest_log_version_to_discard) break;
      log_versions_.pop_front();

      // `fs::remove()` returns true if the file exists and was removed.
      if (!fs::remove(LogPathForVersion(oldest_version))) {
        // The log file may not exist if no writes were made to it.
        continue;
      }

      // We need to sync the directory before continuing to make sure the log
      // file removal has reached persistent storage. Otherwise a crash may
      // introduce data inconsistencies during log replay.
      if (fsync(log_dir_fd_) != 0) {
        return Status::FromPosixError(log_dir_path_.string(), errno);
      }
    }
    return Status::OK();

  } catch (const fs::filesystem_error& ex) {
    return Status::FromPosixError(log_dir_path_.string(), ex.code().value());
  }
}

Status Manager::DiscardOldest(const uint64_t newest_log_version_to_discard,
                              std::unique_lock<std::mutex>* lock) {
  assert(mode_ == Mode::kWrite);
  assert(lock == nullptr || lock->owns_lock());

  // If there are no immutable log versions, or the oldest version is newer than
  // `newest_log_version_to_discard`, there is nothing to do.
  if (log_versions_.empty() ||
      (log_versions_.front() > newest_log_version_to_discard)) {
    if (lock != nullptr) lock->unlock();
    return Status::OK();
  }

  try {
    const uint64_t oldest_version = log_versions_.front();
    // `fs::remove()` returns true if the file exists and was removed.
    if (!fs::remove(LogPathForVersion(oldest_version))) {
      if (lock != nullptr) lock->unlock();
      // The log file may not exist if no writes were made to it.
      return Status::OK();
    }
  } catch (const fs::filesystem_error& ex) {
    if (lock != nullptr) lock->unlock();
    return Status::FromPosixError(log_dir_path_.string(), ex.code().value());
  }

  // Log file removal succeeded; now we can remove the version from
  // `log_versions_`.
  log_versions_.pop_front();
  if (lock != nullptr) lock->unlock();

  // We need to sync the directory before continuing to make sure the log
  // file removal has reached persistent storage. Otherwise a crash may
  // introduce data inconsistencies during log replay.
  if (fsync(log_dir_fd_) != 0) {
    return Status::FromPosixError(log_dir_path_.string(), errno);
  }
  return Status::OK();
}

Status Manager::DiscardAllForCleanShutdown() {
  if (mode_ != Mode::kWrite) {
    // No-op.
    return Status::OK();
  }

  // Shuts down the current writer.
  writer_.reset();
  log_versions_.push_back(active_log_version_);
  return DiscardUpToInclusive(active_log_version_);
}

Status Manager::OpenAndCollectLogVersions() {
  Status status = OpenLogDir();
  if (!status.ok()) return status;
  return CollectLogVersions();
}

Status Manager::OpenLogDir() {
  // Attempt to create a directory if needed.
  try {
    if (!fs::is_directory(log_dir_path_)) {
      fs::create_directory(log_dir_path_);
    }
  } catch (const fs::filesystem_error& ex) {
    return Status::FromPosixError(log_dir_path_.string(), ex.code().value());
  }

  // Get a file descriptor for the directory (needed to `fsync()` the
  // directory).
  log_dir_fd_ = open(log_dir_path_.c_str(), O_DIRECTORY | O_RDONLY);
  if (log_dir_fd_ < 0) {
    return Status::FromPosixError(log_dir_path_.string(), errno);
  }
  return Status::OK();
}

Status Manager::CollectLogVersions() {
  std::vector<uint64_t> versions;
  uint64_t parsed_version = 0;
  try {
    for (const auto& dir_entry : fs::directory_iterator(log_dir_path_)) {
      if (!dir_entry.is_regular_file()) continue;
      // Log file names are integers (the version) - try to parse them.
      if (!ParseLogVersion(dir_entry.path().filename().string(),
                           &parsed_version)) {
        continue;
      }
      versions.push_back(parsed_version);
    }
  } catch (const fs::filesystem_error& ex) {
    return Status::FromPosixError(log_dir_path_.string(), ex.code().value());
  }

  // The directory iterator has no ordering guarantees, so we sort the vector
  // first before inserting into `log_versions_`.
  std::sort(versions.begin(), versions.end());
  log_versions_.clear();
  log_versions_.insert(log_versions_.end(), versions.begin(), versions.end());
  return Status::OK();
}

fs::path Manager::LogPathForVersion(uint64_t version) const {
  return log_dir_path_ / std::to_string(version);
}

}  // namespace wal
}  // namespace tl
