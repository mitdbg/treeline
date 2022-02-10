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

  # Extract the workload path
  if [[ $phys_arg =~ --workload_config=.+ ]]; then
    workload_path=${phys_arg:18}
  fi

  # Extract the checkpoint name, which shouldn't be passed as an argument further.
  # Add anything else to args.
  if [[ $phys_arg =~ --checkpoint_name=.+ ]]; then
    checkpoint_name=${phys_arg:18}
  else
    args+=($phys_arg)
  fi
done

if [[ -z $checkpoint_name || -z $workload_path ]]; then
  echo >&2 "Usage: $0 --checkpoint_name=<checkpoint name> --workload_config=<workload path> [other args passed to pg_bench]"
  exit 1
fi

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name

# Check if the checkpoint already exists. If so, we do not need to rerun.
if [ -d "$full_checkpoint_path" ]; then
  echo >&2 "Checkpoint $checkpoint_name already exists. No need to recreate."
  exit 0
fi

mkdir -p $DB_CHECKPOINT_PATH

echo >&2 "Creating the database..."

../../build/page_grouping/pg_bench \
  --db_path=$full_checkpoint_path \
  --workload_config=$workload_path \
  --seed=$SEED \
  --output_path=$COND_OUT \
  --verbose \
  --use_memory_based_io \
  ${args[@]}

echo >&2 "Done loading. Shuffling the pages now..."

../../build/page_grouping/pg_shuffle \
  --db_path=$full_checkpoint_path \
  --seed=$SEED
