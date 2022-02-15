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

  # Extract the database type (e.g., llsm, rocksdb)
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

# Generates the workload configuration and filters out unrelated arguments.
args=$(python3 generate_workload.py ${all_args[@]})

echo "Detected DB Type: $db_type"
echo "Detected checkpoint name: $checkpoint_name"

# Add additional common arguments.
args+=("--verbose")
args+=("--db_path=$DB_PATH")
args+=("--seed=$SEED")
args+=("--notify_after_init")
args+=("--skip_load")

# Use the generated workload file.
args+=("--workload_config=$COND_OUT/workload.yml")

# Copy over the checkpoint.
full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name
rm -rf $DB_PATH
cp -r $full_checkpoint_path $DB_PATH
sync $DB_PATH

init_finished=0

function on_init_finish() {
  init_finished=1
}

# Interrupt the first `wait` below when we receive a `SIGUSR1` signal.
trap "on_init_finish" USR1

set +e
../../build/bench/run_custom ${args[@]} >$COND_OUT/results.csv &
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

cp $DB_PATH/$db_type/LOG $COND_OUT/$db_type.log
du -b $DB_PATH >$COND_OUT/db_space.log

# Report that the experiment failed if the `run_custom` exit code is not 0
if [ $code -ne 0 ]; then
  exit $code
fi
