#pragma once

#include "gflags/gflags.h"

DECLARE_string(db_path);
DECLARE_bool(disable_segments);
DECLARE_uint32(records_per_page_goal);
DECLARE_double(records_per_page_epsilon);
DECLARE_uint32(bg_threads);
DECLARE_bool(use_memory_based_io);
DECLARE_uint32(write_batch_size);
