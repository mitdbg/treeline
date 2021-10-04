#! /bin/bash

# Acknowledgement:
# https://stackoverflow.com/questions/25288194/dont-display-pushd-popd-stack-across-several-bash-scripts-quiet-pushd-popd
pushd () {
  command pushd "$@" > /dev/null
}
popd () {
  command popd "$@" > /dev/null
}

script_loc=$(cd $(dirname $0) && pwd -P)
pushd $script_loc
source fio_config.sh
popd

mkdir -p $FIO_OUT_DIR

# All jobs in a jobfile share the same physical file.
fio \
  --output=$COND_OUT/fio.json \
  --output-format=json \
  --directory=$FIO_OUT_DIR \
  $@
