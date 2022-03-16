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
  fi

  # Extract goal/delta.
  if [[ $phys_arg =~ --records_per_page_goal=.+ ]]; then
    goal=${phys_arg:24}
  fi
  if [[ $phys_arg =~ --records_per_page_delta=.+ ]]; then
    delta=${phys_arg:25}
  fi

  # Extract the checkpoint name, which shouldn't be passed as an argument further.
  # Add anything else to args.
  if [[ $phys_arg =~ --checkpoint_name=.+ ]]; then
    checkpoint_name=${phys_arg:18}
  else
    args+=($phys_arg)
  fi
done

if [[ -z $checkpoint_name ]]; then
  echo >&2 "Usage: $0 --checkpoint_name=<checkpoint name> [other args passed to run_custom]"
  exit 1
fi

if [ -z $COND_OUT ]; then
  echo >&2 "ERROR: This script is meant to be launched by Conductor."
  exit 1
fi

echo >&2 "Detected DB Type: $db_type"
echo >&2 "Detected checkpoint name: $checkpoint_name"

# Add common arguments.
args+=("--verbose")
args+=("--db_path=$DB_PATH")
args+=("--seed=$SEED")
args+=("--skip_load")

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name
rm -rf $DB_PATH
cp -r $full_checkpoint_path $DB_PATH
sync $DB_PATH

# 1. Run the "no_alloc" case (i.e., without perfect allocation).

mkdir -p $COND_OUT/no_alloc

../../build/bench/run_custom \
  ${args[@]} \
  --output_path=$COND_OUT/no_alloc \
  > $COND_OUT/no_alloc/results.csv
code=$?

if [ -e $DB_PATH/$db_type/LOG ]; then
  cp $DB_PATH/$db_type/LOG $COND_OUT/no_alloc/$db_type.log
fi
du -b $DB_PATH >$COND_OUT/no_alloc/db_space.log

# Report that the experiment failed if the `run_custom` exit code is not 0
if [ $code -ne 0 ]; then
  exit $code
fi

# The "perfect allocation" case is only for pg_llsm.
if [ $db_type != "pg_llsm" ]; then
  exit 0
fi

# 2. Run the "perfect_alloc" case.

mkdir -p $COND_OUT/perfect_alloc

# Remove all overflows.
../../build/page_grouping/pg_flatten --db_path=$DB_PATH --goal=$goal --delta=$delta

# Run again, this time with "perfect allocation".
../../build/bench/run_custom \
  ${args[@]} \
  --output_path=$COND_OUT/perfect_alloc \
  > $COND_OUT/perfect_alloc/results.csv
code=$?
