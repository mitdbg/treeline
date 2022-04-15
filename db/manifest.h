#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

#include "treeline/status.h"

namespace tl {

// Holds database metadata that is needed when initializing an existing TL
// database. This class is mainly meant to help with serializing and
// deserializing the manifest.
class Manifest {
 public:
  class Builder;

  // Loads the manifest from `manifest_file`. If there was an error loading the
  // manifest, `status` will be set accordingly and the returned optional will
  // not contain a value.
  static std::optional<Manifest> LoadFrom(
      const std::filesystem::path& manifest_file, Status* status_out);

  // Writes the manifest to `manifest_file` and ensures it is persisted. This
  // method will call `fdatasync()` on the file and `fsync()` on the directory.
  Status WriteTo(const std::filesystem::path& manifest_file) const;

  uint64_t num_pages() const { return num_pages_; }
  uint64_t num_segments() const { return num_segments_; }

 private:
  friend class Builder;
  Manifest(uint64_t num_pages, uint64_t num_segments)
      : num_pages_(num_pages),
        num_segments_(num_segments)
       {}

  const uint64_t num_pages_;
  const uint64_t num_segments_;
};

// A helper class for constructing a `Manifest`.
class Manifest::Builder {
 public:
  Builder() : num_pages_(0), num_segments_(0) {}
  Builder&& WithNumPages(uint64_t num_pages) {
    num_pages_ = num_pages;
    return std::move(*this);
  }
  Builder&& WithNumSegments(uint64_t num_segments) {
    num_segments_ = num_segments;
    return std::move(*this);
  }
  Manifest Build() && {
    return Manifest(num_pages_, num_segments_);
  }

 private:
  uint64_t num_pages_;
  uint64_t num_segments_;
};

}  // namespace tl
