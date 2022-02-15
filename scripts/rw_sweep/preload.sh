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
  fi

  # Extract the checkpoint name, which shouldn't be passed as an argument further.
  # Add anything else to args.
  if [[ $phys_arg =~ --checkpoint_name=.+ ]]; then
    checkpoint_name=${phys_arg:18}
  else
    all_args+=($phys_arg)
  fi
done

if [[ -z $db_type || -z $checkpoint_name ]]; then
  echo >&2 "Usage: $0 --checkpoint_name=<checkpoint name> --db=<all|rocksdb|llsm> [other args passed to run_custom]"
  exit 1
fi

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name

# Check if the checkpoint already exists. If so, we do not need to rerun.
if [ -d "$full_checkpoint_path" ]; then
  echo >&2 "Checkpoint $checkpoint_name already exists. No need to recreate."
  exit 0
fi

mkdir -p $DB_CHECKPOINT_PATH

# Generates the workload configuration and filters out unrelated arguments.
args=$(python3 generate_workload.py ${all_args[@]})

if [ $db_type == "rocksdb" ] || [ $db_type == "all" ]; then
  # RocksDB - Important to use 64 MiB memtables to ensure the emitted sstables
  #           are small too.
  ../../build/bench/run_custom \
    --db=rocksdb \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --memtable_size_mib=64 \
    --workload_config=$COND_OUT/workload.yml \
    --use_direct_io=true \
    --seed=$SEED \
    --verbose \
    --rdb_bloom_bits=10 \
    ${args[@]}
fi

if [ $db_type == "llsm" ] || [ $db_type == "all" ]; then
  # LLSM - Use a larger memtable to help the load run faster.
  ../../build/bench/run_custom \
    --db=llsm \
    --db_path=$full_checkpoint_path \
    --bg_threads=16 \
    --bypass_wal=true \
    --llsm_page_fill_pct=70 \
    --workload_config=$COND_OUT/workload.yml \
    --seed=$SEED \
    --verbose \
    --use_alex=false \
    ${args[@]}
fi
