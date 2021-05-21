#! /bin/bash
set -e

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

if [ -z $1 ]; then
  >&2 echo "Usage: $0 <checkpoint name> [other args passed to run_custom]"
  exit 1
fi

checkpoint_name=$1
shift 1

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name

# Check if the checkpoint already exists. If so, we do not need to rerun.
if [ -d "$full_checkpoint_path" ]; then
  >&2 echo "Checkpoint $checkpoint_name already exists. No need to recreate."
  exit 0
fi

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=$@
all_args=()
for val in "${orig_args[@]}"; do
  all_args+=$(eval "echo $val")
done

mkdir -p $DB_CHECKPOINT_PATH

# Generates the workload configuration and filters out unrelated arguments.
args=$(python3 generate_workload.py $all_args)

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
  $args

# LLSM - Use a larger memtable to help the load run faster.
../../build/bench/run_custom \
  --db=llsm \
  --db_path=$full_checkpoint_path \
  --bg_threads=16 \
  --bypass_wal=true \
  --llsm_page_fill_pct=50 \
  --memtable_size_mib=2048 \
  --workload_config=$COND_OUT/workload.yml \
  --seed=$SEED \
  --verbose \
  $args
