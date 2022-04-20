#include "manifest.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <limits>

#include "util/coding.h"
#include "util/crc32c.h"

namespace {

// All manifest files start with these two bytes.
const std::string kSignature = u8"TL";

// The current manifest file format version. This value should be incremented
// when a breaking change is made to the file format.
constexpr uint32_t kFormatVersion = 3;

// Manifest file format
// ====================
// [Header]
// Signature (2 bytes)
// Format version (uint32; 4 bytes)
// Payload size (uint32; 4 bytes)
// CRC32C checksum of the payload (4 bytes)
//
// [Payload]
// Number of pages (uint64; 8 bytes)
// Number of segments (uint64; 8 bytes)

constexpr size_t kHeaderSize = 14;
constexpr size_t kFormatVersionOffset = 2;
constexpr size_t kPayloadSizeOffset = 6;
constexpr size_t kChecksumOffset = 10;
constexpr size_t kPayloadOffset = kHeaderSize;

}  // namespace

namespace tl {

std::optional<Manifest> Manifest::LoadFrom(
    const std::filesystem::path& manifest_file, Status* status_out) {
  std::ifstream in(manifest_file, std::ios_base::in | std::ios_base::binary);
  std::string buffer;

  // Load the header.
  buffer.resize(kHeaderSize);
  in.read(buffer.data(), kHeaderSize);
  if (in.fail()) {
    *status_out = Status::IOError("Failed to read manifest header.");
    return std::optional<Manifest>();
  }

  // Validate the signature.
  Slice header(buffer);
  if (!header.starts_with(Slice(kSignature))) {
    *status_out = Status::Corruption("Invalid manifest file signature.");
    return std::optional<Manifest>();
  }
  header.remove_prefix(kSignature.size());

  // Validate the format version.
  const uint32_t format_version = DecodeFixed32(header.data());
  if (format_version != kFormatVersion) {
    *status_out =
        Status::NotSupported("Manifest format version is unsupported:",
                             std::to_string(format_version));
    return std::optional<Manifest>();
  }
  header.remove_prefix(sizeof(uint32_t));

  // Decode the payload size and expected checksum.
  const uint32_t payload_size = DecodeFixed32(header.data());
  header.remove_prefix(4);
  const uint32_t expected_checksum = DecodeFixed32(header.data());
  header.remove_prefix(4);

  // Should be done parsing the header.
  assert(header.size() == 0);

  // Load the rest of the payload.
  buffer.resize(payload_size);
  in.read(buffer.data(), payload_size);
  if (in.fail()) {
    *status_out = Status::IOError("Failed to read manifest payload.");
    return std::optional<Manifest>();
  }

  // Validate the checksum.
  const uint32_t computed_checksum = crc32c::Value(
      reinterpret_cast<const uint8_t*>(buffer.data()), payload_size);
  if (computed_checksum != expected_checksum) {
    *status_out = Status::Corruption("Manifest checksum does not match.");
    return std::optional<Manifest>();
  }

  // Decode the rest of the manifest data.
  Slice payload(buffer);
  const uint64_t num_pages = DecodeFixed64(payload.data());
  payload.remove_prefix(8);
  const uint64_t num_segments = DecodeFixed64(payload.data());
  payload.remove_prefix(8);

  *status_out = Status::OK();
  return std::optional<Manifest>(Manifest(num_pages, num_segments));
}

Status Manifest::WriteTo(const std::filesystem::path& manifest_file) const {
  std::string buffer;
  buffer.append(kSignature);
  PutFixed32(&buffer, kFormatVersion);
  PutFixed32(&buffer, 0);  // Payload length placeholder
  PutFixed32(&buffer, 0);  // Checksum placeholder
  assert(buffer.size() == kHeaderSize);

  PutFixed64(&buffer, num_pages_);
  PutFixed64(&buffer, num_segments_);

  const size_t payload_size = buffer.size() - kHeaderSize;
  assert(payload_size <= std::numeric_limits<uint32_t>::max());
  EncodeFixed32(&buffer[kPayloadSizeOffset], payload_size);

  const uint32_t checksum = crc32c::Value(
      reinterpret_cast<const uint8_t*>(&buffer[kPayloadOffset]), payload_size);
  EncodeFixed32(&buffer[kChecksumOffset], checksum);

  // Write the manifest data (will overwrite).
  {
    std::ofstream out(manifest_file, std::ios_base::out |
                                         std::ios_base::binary |
                                         std::ios_base::trunc);
    out.write(buffer.data(), buffer.size());
    out.flush();
    if (out.fail()) {
      return Status::IOError("Failed to write DB manifest");
    }
  }

  // Sync the file and directory.
  const int manifest_fd = open(manifest_file.c_str(), O_RDONLY);
  if (manifest_fd < 0) {
    return Status::FromPosixError("Opening DB manifest:", errno);
  }
  if (fdatasync(manifest_fd) < 0) {
    const int err_code = errno;
    close(manifest_fd);
    return Status::FromPosixError("Syncing DB manifest:", err_code);
  }
  close(manifest_fd);

  // Ensure the manifest file entry has made it to persistent storage.
  const int db_dir_fd =
      open(manifest_file.parent_path().c_str(), O_DIRECTORY | O_RDONLY);
  if (db_dir_fd < 0) {
    return Status::FromPosixError("Opening DB directory:", errno);
  }
  if (fsync(db_dir_fd) < 0) {
    const int err_code = errno;
    close(db_dir_fd);
    return Status::FromPosixError("Syncing DB directory manifest file:",
                                  err_code);
  }
  close(db_dir_fd);

  return Status::OK();
}

}  // namespace tl
