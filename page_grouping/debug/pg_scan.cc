#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "../key.h"
#include "../manager.h"
#include "gflags/gflags.h"
#include "llsm/slice.h"

DEFINE_string(db_path, "", "Path to an existing page-grouped DB.");
DEFINE_uint32(goal, 45, "Records per page goal.");
DEFINE_uint32(delta, 5, "Records per page delta.");
DEFINE_bool(use_segments, true, "Set to false to use pages only.");
DEFINE_string(dump_keys_to, "", "Write out the scanned keys to a file.");

using namespace llsm;
using namespace llsm::pg;

namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_db_path.empty()) {
    std::cerr << "ERROR: Must provide a path to an existing DB." << std::endl;
    return 1;
  }

  const fs::path db_path(FLAGS_db_path);
  Manager::Options options;
  options.records_per_page_goal = FLAGS_goal;
  options.records_per_page_delta = FLAGS_delta;
  options.use_segments = FLAGS_use_segments;
  options.use_memory_based_io = true;
  Manager m = Manager::Reopen(db_path, options);

  // Do a full DB scan.
  std::vector<std::pair<Key, std::string>> scanned_records;
  m.Scan(0, 100000000ULL, &scanned_records);

  std::cerr << "Scanned " << scanned_records.size() << " records." << std::endl;

  if (!FLAGS_dump_keys_to.empty()) {
    std::ofstream out(FLAGS_dump_keys_to);
    for (const auto& r : scanned_records) {
      out << r.first << std::endl;
    }
  }

  return 0;
}
