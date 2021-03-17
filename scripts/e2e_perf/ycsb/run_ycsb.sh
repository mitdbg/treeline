#! /bin/bash
set -e

if [ -z $1 ]; then
  echo "Usage: $0 <checkpoint name> [...other args passed to ./bench/ycsb]"
  exit 1
fi

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc/..
source experiment_config.sh

checkpoint_name=$1
full_checkpoint_path=$DB_CHECKPOINT_PATH/$checkpoint_name
shift 1

rm -rf $DB_PATH && mkdir -p $DB_PATH
cp -r $full_checkpoint_path/llsm $DB_PATH

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=$@
args=()
for val in "${orig_args[@]}"; do
  args+=$(eval "echo $val")
done

cd ../../build
./bench/ycsb \
  --db=llsm \
  --db_path=$DB_PATH \
  --verbose \
  $args \
  > $COND_OUT/ycsb.csv
