#include "load_data.h"

#include <fstream>
#include <iostream>
#include <unordered_set>

namespace tl {
namespace bench {

std::vector<uint64_t> LoadDatasetFromTextFile(
    const std::filesystem::path& dataset_path, const bool warn_on_duplicates) {
  std::unordered_set<uint64_t> keys;
  std::ifstream in(dataset_path);
  uint64_t loaded_key;
  while (in >> loaded_key) {
    auto result = keys.insert(loaded_key);
    if (warn_on_duplicates && !result.second) {
      std::cerr << "WARNING: Ignored a duplicate key in the dataset: "
                << loaded_key << std::endl;
    }
  }

  std::vector<uint64_t> keys_as_vector(keys.begin(), keys.end());
  return keys_as_vector;
}

}  // namespace bench
}  // namespace tl
