#include <cstdint>
#include <iostream>
#include <limits>

#include "gflags/gflags.h"
#include "treeline/pg_db.h"

DEFINE_string(db_path, "", "Path to the database directory.");
DEFINE_uint64(
    start_key, 1,
    "The lower boundary of the key space to pass to `FlattenRange()`.");
DEFINE_uint64(end_key, std::numeric_limits<uint64_t>::max(),
              "The upper boundary (exclusive) of the key space to pass to "
              "`FlattenRange()`.");
DEFINE_uint32(goal, 44,
              "Passed to PageGroupedDBOptions::records_per_page_goal.");
DEFINE_double(epsilon, 5,
              "Passed to PageGroupedDBOptions::records_per_page_epsilon.");

using namespace tl;
using namespace tl::pg;

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Runs FlattenRange() on a page-grouped database.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_db_path.empty()) {
    std::cerr << "ERROR: Must provide a database path." << std::endl;
    return 1;
  }

  PageGroupedDBOptions options;
  options.use_segments = true;
  options.records_per_page_goal = FLAGS_goal;
  options.records_per_page_epsilon = FLAGS_epsilon;
  options.use_memory_based_io = true;

  std::cerr << "Flattening PGTreeLine DB at: " << FLAGS_db_path << std::endl;
  PageGroupedDB* db = nullptr;
  PageGroupedDB::Open(options, FLAGS_db_path, &db);
  assert(db != nullptr);
  const auto status = db->FlattenRange(FLAGS_start_key, FLAGS_end_key);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  delete db;

  return 0;
}
