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

  # Extract the workload path
  if [[ $phys_arg =~ --workload_config=.+ ]]; then
    workload_path=${phys_arg:18}
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

if [[ -z $db_type || -z $checkpoint_name || -z $workload_path ]]; then
  echo >&2 "Usage: $0 --checkpoint_name=<checkpoint name> --workload_config=<workload path> --db=<all|rocksdb|llsm|pg_llsm|leanstore> [other args passed to run_custom]"
  exit 1
fi

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name

# Check if the checkpoint already exists. If so, we do not need to rerun.
if [ -d "$full_checkpoint_path" ]; then
  echo >&2 "Checkpoint $checkpoint_name already exists. No need to recreate."
  exit 0
fi

mkdir -p $DB_CHECKPOINT_PATH

if [ $db_type == "rocksdb" ] || [ $db_type == "all" ]; then
  # RocksDB - Important to use 64 MiB memtables to ensure the emitted sstables
  #           are small too.
  ../../build/bench/run_custom \
    ${args[@]} \
    --db=rocksdb \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --memtable_size_mib=64 \
    --workload_config=$workload_path \
    --use_direct_io=true \
    --seed=$SEED \
    --verbose
fi

if [ $db_type == "llsm" ] || [ $db_type == "all" ]; then
  # LLSM - Use a larger memtable to help the load run faster.
  ../../build/bench/run_custom \
    ${args[@]} \
    --db=llsm \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --tl_page_fill_pct=50 \
    --memtable_size_mib=2048 \
    --workload_config=$workload_path \
    --seed=$SEED \
    --verbose
fi

if [ $db_type == "pg_llsm" ] || [ $db_type == "all" ]; then
  # Page-grouped LLSM - Load in three steps. We use "memory-based I/O" to speed
  # up the load process.

  # 1. Bulk load the database.
  ../../build/bench/run_custom \
    ${args[@]} \
    --db=pg_llsm \
    --bg_threads=16 \
    --db_path=$full_checkpoint_path \
    --workload_config=$workload_path \
    --pg_use_memory_based_io \
    --seed=$SEED \
    --verbose \
    --skip_workload

  # 2. Shuffle the on-disk pages.
  echo >&2 "Done loading. Shuffling the pages now..."
  ../../build/page_grouping/pg_shuffle \
    --db_path=$full_checkpoint_path/pg_llsm \
    --seed=$SEED

  # 3. Run the preload workload.
  echo >&2 "Done shuffling. Running the setup workload now..."
  ../../build/bench/run_custom \
    ${args[@]} \
    --db=pg_llsm \
    --bg_threads=16 \
    --db_path=$full_checkpoint_path \
    --workload_config=$workload_path \
    --pg_use_memory_based_io \
    --seed=$SEED \
    --verbose \
    --skip_load \
    --output_path=$COND_OUT

  # Store a copy of the database debug information if it exists.
  if [ -d $COND_OUT ] && [ -d "$full_checkpoint_path/debug" ]; then
    cp -r $full_checkpoint_path/debug $COND_OUT
  fi
fi

if [ $db_type == "leanstore" ] || [ $db_type == "all" ]; then
  # Leanstore
  ../../build/bench/run_custom \
    ${args[@]} \
    --db=leanstore \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --workload_config=$workload_path \
    --seed=$SEED \
    --verbose \
    --persist 
fi
