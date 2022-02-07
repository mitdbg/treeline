#pragma once

#include "gflags/gflags.h"

DECLARE_string(db_path);
DECLARE_bool(disable_segments);
DECLARE_uint32(records_per_page_goal);
DECLARE_uint32(records_per_page_delta);
DECLARE_uint32(bg_threads);
