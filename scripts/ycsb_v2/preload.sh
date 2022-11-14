#! /bin/bash
set -e

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=($@)
all_args=()
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
  all_args+=($phys_arg)
done

if [[ -z $db_type || -z $checkpoint_name ]]; then
  echo >&2 "Usage: $0 --checkpoint_name=<checkpoint name> --db=<all|rocksdb|llsm|pg_llsm|leanstore> [other args passed to run_custom]"
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

# Generate the preload workload configuration file and filter out unrelated arguments.
# The generated workload configuration file will be saved to $COND_OUT/workload.yml.
args=$(python3 generate_workload.py ${all_args[@]})

mkdir -p $DB_CHECKPOINT_PATH

if [ $db_type == "rocksdb" ] || [ $db_type == "all" ]; then
  # RocksDB
  ../../build/bench/run_custom \
    ${args[@]} \
    --db=rocksdb \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --workload_config=$COND_OUT/workload.yml \
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
    --workload_config=$COND_OUT/workload.yml \
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
    --workload_config=$COND_OUT/workload.yml \
    --pg_use_memory_based_io \
    --seed=$SEED \
    --verbose \
    --skip_workload

  if [ -d $full_checkpoint_path/pg_tl ]; then
    tl_path=$full_checkpoint_path/pg_tl
  else
    tl_path=$full_checkpoint_path/pg_llsm
  fi

  # 2. Shuffle the on-disk pages.
  echo >&2 "Done loading. Shuffling the pages now..."
  ../../build/page_grouping/pg_shuffle \
    --db_path=$tl_path \
    --seed=$SEED

  # 3. Run the preload workload.
  echo >&2 "Done shuffling. Running the setup workload now..."
  ../../build/bench/run_custom \
    ${args[@]} \
    --db=pg_llsm \
    --bg_threads=16 \
    --db_path=$full_checkpoint_path \
    --workload_config=$COND_OUT/workload.yml \
    --pg_use_memory_based_io \
    --seed=$SEED \
    --verbose \
    --skip_load \
    --output_path=$COND_OUT

  # 4. Verify that the DB's physical files are consistent.
  ../../build/page_grouping/debug/pg_check --db_path=$tl_path

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
    --workload_config=$COND_OUT/workload.yml \
    --seed=$SEED \
    --verbose \
    --persist 
fi
