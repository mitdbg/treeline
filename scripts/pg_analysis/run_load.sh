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
done

if [ -z $COND_OUT ]; then
  echo >&2 "ERROR: This script is meant to be launched by Conductor."
  exit 1
fi

# Used when multiple experiments are running at the same time.
if [ ! -z $COND_SLOT ]; then
  full_db_path=$DB_PATH-$COND_SLOT
else
  full_db_path=$DB_PATH
fi

rm -rf $full_db_path
mkdir $full_db_path
sync $full_db_path

# Add common arguments.
args+=("--verbose")
args+=("--db_path=$full_db_path")
args+=("--seed=$SEED")

set +e
../../build/bench/run_custom \
  --db=pg_llsm \
  ${args[@]} \
  --workload_config=workload.yml \
  --output_path=$COND_OUT \
  > $COND_OUT/results.csv
code=$?
set -e

du -b $full_db_path > $COND_OUT/db_space.log

# Store the generated debug information. Note that this information is only
# generated when segments are used. We retrieve the number of pages in the
# non-segment case by consulting the debug counters file.
if [ -d "${full_db_path}/pg_llsm/debug" ]; then
  cp -r $full_db_path/pg_llsm/debug $COND_OUT
elif [ -d "${full_db_path}/pg_tl/debug" ]; then
  cp -r $full_db_path/pg_tl/debug $COND_OUT
fi

# Report that the experiment failed if the `run_custom` exit code is not 0
if [ $code -ne 0 ]; then
  exit $code
fi
