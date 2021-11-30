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

device=$1
jobfile=$2
shift 2

args=()
args+=("--output=$COND_OUT/fio.json")
args+=("--output-format=json")
if [ $device = "nvme_ssd_raw" ]; then
  args+=("--directory=$NVME_SSD_RAW_OUT_DIR")
  args+=("--filename=$NVME_SSD_RAW_FILE")
else
  args+=("--directory=$NVME_SSD_FS_OUT_DIR")
  args+=("--filename=$jobfile")
  mkdir -p $NVME_SSD_FS_OUT_DIR
  rm -f $NVME_SSD_FS_OUT_DIR/$jobfile
fi
args+=($jobfile)

# All jobs in a jobfile share the same physical file.
fio ${args[@]}
