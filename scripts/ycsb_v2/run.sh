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

# Generate the workload configuration file and filter out unrelated arguments.
# The generated workload configuration file will be saved to $COND_OUT/workload.yml.
args=$(python3 generate_workload.py ${all_args[@]})

# Add common arguments.
args+=("--verbose")
args+=("--db_path=$DB_PATH")
args+=("--seed=$SEED")
args+=("--notify_after_init")

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name
rm -rf $DB_PATH

args+=("--skip_load")
cp -r $full_checkpoint_path $DB_PATH

# Update leanstore configuration file to reflect moving the database.
if [ -e $DB_PATH/$db_type/leanstore.json ]; then
  sed -i 's|'"$full_checkpoint_path"'|'"$DB_PATH"'|g' $DB_PATH/$db_type/leanstore.json 
fi

sync $DB_PATH

init_finished=0

function on_init_finish() {
  init_finished=1
}

# Interrupt the first `wait` below when we receive a `SIGUSR1` signal.
trap "on_init_finish" USR1

set +e
../../build/bench/run_custom \
  ${args[@]} \
  --workload_config=$COND_OUT/workload.yml \
  --output_path=$COND_OUT \
  > $COND_OUT/results.csv &
wait %1
code=$?

# The experiment failed before it finished initialization.
if [ "$init_finished" -eq "0" ]; then
  exit $code
fi

# The DB has finished initializing, so start `iostat`.
iostat -o JSON -d -y 1 >$COND_OUT/iostat.json &
iostat_pid=$!

# Wait until the workload completes.
wait %1
code=$?

# Stop `iostat`.
kill -s SIGINT -- $iostat_pid
wait

if [ -e $DB_PATH/$db_type/LOG ]; then
  cp $DB_PATH/$db_type/LOG $COND_OUT/$db_type.log
fi
du -b $DB_PATH >$COND_OUT/db_space.log

# Report that the experiment failed if the `run_custom` exit code is not 0
if [ $code -ne 0 ]; then
  exit $code
fi

# For TreeLine, verify that the DB's physical files are consistent.
if [ $db_type = "pg_llsm" ]; then
  if [ -d $DB_PATH/pg_tl ]; then
    tl_path=$DB_PATH/pg_tl
  else
    tl_path=$DB_PATH/pg_llsm
  fi

  set -e
  ../../build/page_grouping/debug/pg_check --db_path=$tl_path
fi
