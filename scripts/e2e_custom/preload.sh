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
  args+=($phys_arg)

  # Extract the database type (e.g., llsm, rocksdb, leanstore)
  if [[ $phys_arg =~ --db=.+ ]]; then
    db_type=${phys_arg:5}
  fi

  # Extract the checkpoint name
  if [[ $phys_arg =~ --checkpoint_name=.+ ]]; then
    checkpoint_name=${phys_arg:18}
  fi

  # Extract the workload path
  if [[ $phys_arg =~ --workload_config=.+ ]]; then
    workload_path=${phys_arg:18}
  fi
done

if [[ -z $db_type || -z $checkpoint_name || -z $workload_path ]]; then
  echo >&2 "Usage: $0 --checkpoint_name=<checkpoint name> --workload_config=<workload path> --db=<all|rocksdb|llsm> [other args passed to run_custom]"
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
    --db=rocksdb \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --memtable_size_mib=64 \
    --workload_config=$workload_path \
    --use_direct_io=true \
    --seed=$SEED \
    --verbose \
    ${args[@]}
fi

if [ $db_type == "llsm" ] || [ $db_type == "all" ]; then
  # LLSM - Use a larger memtable to help the load run faster.
  ../../build/bench/run_custom \
    --db=llsm \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --llsm_page_fill_pct=50 \
    --memtable_size_mib=2048 \
    --workload_config=$workload_path \
    --seed=$SEED \
    --verbose \
    --use_alex=false \
    ${args[@]}
fi

# LeanStore does not support "reopening" an existing persisted database. So we
# do not preload it here.
