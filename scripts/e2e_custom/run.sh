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

  # Extract the database type (e.g., llsm, rocksdb, leanstore)
  if [[ $phys_arg =~ --db=.+ ]]; then
    db_type=${phys_arg:5}
  fi

  # Extract the checkpoint name
  if [[ $phys_arg =~ --checkpoint_name=.+ ]]; then
    checkpoint_name=${phys_arg:18}
  fi
done

if [[ -z $checkpoint_name ]]; then
  >&2 echo "Usage: $0 --checkpoint_name=<checkpoint name> [other args passed to run_custom]"
  exit 1
fi

echo "Detected DB Type: $db_type"
echo "Detected checkpoint name: $checkpoint_name"

# Add common arguments.
args+=("--verbose")
args+=("--db_path=$DB_PATH")
args+=("--seed=$SEED")

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name
rm -rf $DB_PATH

# LeanStore does not support "reopening" a database. So we do not have
# checkpoints for LeanStore. Note that we currently **do not** run the "setup"
# workload on LeanStore (10 M uniform updates).
if [[ $db_type == "leanstore" ]]; then
  mkdir $DB_PATH
else
  args+=("--skip_load")
  cp -r $full_checkpoint_path $DB_PATH
fi

sync $DB_PATH

set +e
iostat -o JSON -d -y 1 > $COND_OUT/iostat.json &
iostat_pid=$!

../../build/bench/run_custom ${args[@]} > $COND_OUT/results.csv
code=$?

cp $DB_PATH/$db_type/LOG $COND_OUT/$db_type.log
du -b $DB_PATH > $COND_OUT/db_space.log

kill -s SIGINT -- $iostat_pid
wait

# Report that the experiment failed if the `run_custom` exit code is not 0
if [ $code -ne 0 ]; then
  exit $code
fi
