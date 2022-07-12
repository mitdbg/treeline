#include "config.h"

DEFINE_string(db_path, "", "The path where the database(s) should be stored.");
DEFINE_bool(disable_segments, false,
            "If set, the initial bulk load will not create segments.");
DEFINE_uint32(records_per_page_goal, 44,
              "Aim to put this many records on a page.");
DEFINE_double(
    records_per_page_epsilon, 5,
    "The number of records on a page can vary by +/- two times this value.");
DEFINE_uint32(bg_threads, 16,
              "The number of background threads to use (for I/O).");
DEFINE_bool(use_memory_based_io, false,
            "Set to disable direct I/O AND to disable synchronous writes. This "
            "should NOT be set when running actual performance benchmarks.");
DEFINE_uint32(write_batch_size, 1000000,
              "The number of records to batch before initiating a write.");
