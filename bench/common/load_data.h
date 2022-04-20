#pragma once

#include <cstdint>
#include <filesystem>
#include <vector>

namespace tl {
namespace bench {

// The expected file format is a sequence of integers separated by newlines.
// This function will remove any duplicates from the loaded dataset.
std::vector<uint64_t> LoadDatasetFromTextFile(
    const std::filesystem::path& dataset_path, bool warn_on_duplicates = false);

}  // namespace bench
}  // namespace tl
