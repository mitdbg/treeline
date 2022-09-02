#! /bin/bash
set -e

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=($@)
args=()
no_alloc_only=false
for val in "${orig_args[@]}"; do
  phys_arg=$(eval "echo $val")

  # Extract the database type (e.g., llsm, rocksdb, leanstore)
  if [[ $phys_arg =~ --db=.+ ]]; then
    db_type=${phys_arg:5}
  fi

  # Extract goal/epsilon.
  if [[ $phys_arg =~ --records_per_page_goal=.+ ]]; then
    goal=${phys_arg:24}
  fi
  if [[ $phys_arg =~ --records_per_page_epsilon=.+ ]]; then
    epsilon=${phys_arg:27}
  fi

  # Skip the "perfect allocation" part of the experiment.
  if [[ $phys_arg = --no_alloc_only ]]; then
    no_alloc_only=true
    continue
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
args+=("--notify_after_init")

init_finished=0

function on_init_finish() {
  init_finished=1
}

# Interrupt the first `wait` below when we receive a `SIGUSR1` signal.
trap "on_init_finish" USR1

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name
rm -rf $DB_PATH
cp -r $full_checkpoint_path $DB_PATH
sync $DB_PATH

##################################################################################

# 1. Run the "no_alloc" case (i.e., without perfect allocation).
mkdir -p $COND_OUT/no_alloc

echo >&2 "Running the regular insert heavy experiment..."
set +e
../../build/bench/run_custom \
  ${args[@]} \
  --output_path=$COND_OUT/no_alloc \
  > $COND_OUT/no_alloc/results.csv &
wait %1
code=$?

# The experiment failed before it finished initialization.
if [ "$init_finished" -eq "0" ]; then
  exit $code
fi

# The DB has finished initializing, so start `iostat`.
iostat -o JSON -d -y 1 >$COND_OUT/no_alloc/iostat.json &
iostat_pid=$!

# Wait until the workload completes.
wait %1
code=$?

# Stop `iostat`.
kill -s SIGINT -- $iostat_pid
wait
init_finished=0

if [ -e $DB_PATH/$db_type/LOG ]; then
  cp $DB_PATH/$db_type/LOG $COND_OUT/no_alloc/$db_type.log
fi
du -b $DB_PATH >$COND_OUT/no_alloc/db_space.log

# Report that the experiment failed if the `run_custom` exit code is not 0
if [ $code -ne 0 ]; then
  exit $code
fi

# Skip the consistency check and "perfect allocation" case when it is not
# relevant.
if [ $db_type != "pg_llsm" ]; then
  exit 0
fi

tl_path=$DB_PATH/pg_tl
if [ ! -d $tl_path ]; then
  tl_path=$DB_PATH/pg_llsm
fi

set -e
# Verify that the physical DB files are still consistent.
../../build/page_grouping/debug/pg_check --db_path=$tl_path
set +e

# Skip the "perfect allocation" case when it is not relevant.
if [ $no_alloc_only = true ]; then
  exit 0
fi

##################################################################################

# 2. Run the "perfect_alloc" case.
mkdir -p $COND_OUT/perfect_alloc

# Remove all overflows.
echo >&2 "Removing all overflows in the generated DB..."
../../build/page_grouping/pg_flatten \
  --db_path=$tl_path \
  --goal=$goal \
  --epsilon=$epsilon

set -e
# Verify that the physical DB files are still consistent and that the DB has no
# overflow pages.
../../build/page_grouping/debug/pg_check --db_path=$tl_path --ensure_no_overflows
set +e

# Run again, this time with "perfect allocation".
echo >&2 "Now running the \"perfect allocation\" experiment..."
../../build/bench/run_custom \
  ${args[@]} \
  --output_path=$COND_OUT/perfect_alloc \
  > $COND_OUT/perfect_alloc/results.csv &
wait %1
code=$?

# The experiment failed before it finished initialization.
if [ "$init_finished" -eq "0" ]; then
  exit $code
fi

# The DB has finished initializing, so start `iostat`.
iostat -o JSON -d -y 1 >$COND_OUT/perfect_alloc/iostat.json &
iostat_pid=$!

# Wait until the workload completes.
wait %1
code=$?
echo "Done!"

# Stop `iostat`.
kill -s SIGINT -- $iostat_pid
wait

du -b $DB_PATH >$COND_OUT/perfect_alloc/db_space.log

# Report that the experiment failed if the `run_custom` exit code is not 0
if [ $code -ne 0 ]; then
  exit $code
fi
