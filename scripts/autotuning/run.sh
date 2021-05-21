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

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=$@
args=()
for val in "${orig_args[@]}"; do
  args+=$(eval "echo $val")
done

full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name

rm -rf $DB_PATH
cp -r $full_checkpoint_path $DB_PATH
sync $DB_PATH

set +e
iostat -o JSON -d -y 1 > $COND_OUT/iostat.json &
iostat_pid=$!

../../build/bench/run_custom \
  --verbose \
  --db_path=$DB_PATH \
  --seed=$SEED \
  --skip_load \
  --output_path=$COND_OUT \
  $args \
  > $COND_OUT/results.csv

cp $DB_PATH/llsm/LOG $COND_OUT/llsm.log
cp $DB_PATH/rocksdb/LOG $COND_OUT/rocksdb.log

kill -s SIGINT -- $iostat_pid
wait
