#! /bin/bash
set -e

if [ -z $2 ]; then
  echo "Usage: $0 <checkpoint name> <load trace name> [...other args passed to ./bench/ycsb]"
  exit 1
fi

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc/..
source ../experiment_config.sh

checkpoint_name=$1
full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name
trace_name=$2
shift 2

# Check if the checkpoint already exists. If so, we do not need to rerun.
if [ -d "$full_checkpoint_path" ]; then
  >&2 echo "Checkpoint $checkpoint_name already exists. No need to recreate."
  exit 0
fi

mkdir -p $DB_CHECKPOINT_PATH

cd ../../build
./bench/ycsb \
  --db=all \
  --db_path=$full_checkpoint_path \
  --bg_threads=16 \
  --bypass_wal=true \
  --tl_page_fill_pct=50 \
  --memtable_size_mib=64 \
  --load_path=$YCSB_TRACE_PATH/$trace_name \
  $@
