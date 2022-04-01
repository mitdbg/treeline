#include <cstdint>
#include <iostream>
#include <limits>

#include "gflags/gflags.h"
#include "tl/pg_db.h"

DEFINE_string(db_path, "", "Path to the database directory.");
DEFINE_uint64(
    start_key, 0,
    "The lower boundary of the key space to pass to `FlattenRange()`.");
DEFINE_uint64(end_key, std::numeric_limits<uint64_t>::max(),
              "The upper boundary (exclusive) of the key space to pass to "
              "`FlattenRange()`.");
DEFINE_uint32(goal, 45,
              "Passed to PageGroupedDBOptions::records_per_page_goal.");
DEFINE_uint32(delta, 5,
              "Passed to PageGroupedDBOptions::records_per_page_delta.");

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
  options.records_per_page_delta = FLAGS_delta;
  options.use_memory_based_io = true;

  PageGroupedDB* db = nullptr;
  PageGroupedDB::Open(options, FLAGS_db_path, &db);
  assert(db != nullptr);
  db->FlattenRange(FLAGS_start_key, FLAGS_end_key);
  delete db;

  return 0;
}
