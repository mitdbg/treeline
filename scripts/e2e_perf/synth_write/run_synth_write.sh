#! /bin/bash
set -e

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc/..
source ../experiment_config.sh
cd ../../build

# The `synth_write` benchmark executable expects the database output directory
# to not exist.
rm -rf $DB_PATH

# The experiment results will be saved to `results.csv`.
./bench/synth_write \
  --db_path=$DB_PATH \
  $@ \
  > $COND_OUT/results.csv
