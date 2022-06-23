#! /bin/bash
set -e

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

if [ -z $2 ]; then
  >&2 echo "Usage: $0 <checkpoint name> <preload file>"
  exit 1
fi

checkpoint_name=$1
preload_file=$2
shift 2

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name

# Check if the checkpoint already exists. If so, we do not need to rerun.
if [ -d "$full_checkpoint_path" ]; then
  >&2 echo "Checkpoint $checkpoint_name already exists. No need to recreate."
  exit 0
fi

mkdir -p $DB_CHECKPOINT_PATH

# RocksDB - Important to use 64 MiB memtables to ensure the emitted sstables
#           are small too.
../../build/bench/run_custom \
  --db=rocksdb \
  --db_path=$full_checkpoint_path \
  --bg_threads=16 \
  --bypass_wal=true \
  --memtable_size_mib=64 \
  --workload_config=$preload_file \
  --use_direct_io=true \
  --seed=$SEED \
  --verbose

# LLSM - Use a larger memtable to help the load run faster.
../../build/bench/run_custom \
  --db=llsm \
  --db_path=$full_checkpoint_path \
  --bg_threads=16 \
  --bypass_wal=true \
  --tl_page_fill_pct=50 \
  --memtable_size_mib=2048 \
  --workload_config=$preload_file \
  --seed=$SEED \
  --verbose
