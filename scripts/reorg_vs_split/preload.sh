#! /bin/bash
set -e

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=($@)
args=()
for val in "${orig_args[@]}"; do
  phys_arg=$(eval "echo $val")

  # Extract the database type (e.g., llsm, rocksdb, leanstore)
  if [[ $phys_arg =~ --db=.+ ]]; then
    db_type=${phys_arg:5}
    continue
  fi

  # Extract the checkpoint name, which shouldn't be passed as an argument further.
  if [[ $phys_arg =~ --checkpoint_name=.+ ]]; then
    checkpoint_name=${phys_arg:18}
    continue
  fi

  # Add anything else to args.
  args+=($phys_arg)
done

if [[ -z $db_type || -z $checkpoint_name ]]; then
  echo >&2 "Usage: $0 --checkpoint_name=<checkpoint name> --db=<all|rocksdb|llsm> [other args passed to run_custom]"
  exit 1
fi

if [ -z $COND_OUT ]; then
  echo >&2 "ERROR: This script is meant to be launched by Conductor."
  exit 1
fi

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name

# Check if the checkpoint already exists. If so, we do not need to rerun.
if [ -d "$full_checkpoint_path" ]; then
  echo >&2 "Checkpoint $checkpoint_name already exists. No need to recreate."
  exit 0
fi

if [ $db_type != "pg_llsm" ]; then
  echo >&2 "Unsupported DB type $db_type"
  exit 1
fi

mkdir -p $DB_CHECKPOINT_PATH

# Page-grouped TreeLine - Load in two steps. We use "memory-based I/O" to speed
# up the load process.

# 1. Bulk load the database.
../../build/bench/run_custom \
  ${args[@]} \
  --db=pg_llsm \
  --bg_threads=16 \
  --db_path=$full_checkpoint_path \
  --pg_use_memory_based_io \
  --seed=$SEED \
  --verbose \
  --skip_workload

# 2. Shuffle the on-disk pages.
echo >&2 "Done loading. Shuffling the pages now..."
../../build/page_grouping/pg_shuffle \
  --db_path=$full_checkpoint_path/pg_tl \
  --seed=$SEED

# Store a copy of the database debug information if it exists.
if [ -d $COND_OUT ] && [ -d "$full_checkpoint_path/pg_tl/debug" ]; then
  cp -r $full_checkpoint_path/pg_tl/debug $COND_OUT
fi
