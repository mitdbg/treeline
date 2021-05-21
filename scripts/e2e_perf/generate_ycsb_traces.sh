#! /bin/bash

# This script helps generate the YCSB traces used in the LLSM experiments. It
# assumes a few prerequisites:
#
# 1. YCSB has been installed and the `ycsb` executable is available in your `$PATH`.
#    (Install it by following the instructions at https://ycsb.site)
#
# 2. The `ycsb-runner` repository has been cloned and is available on your machine.
#    (Clone it from https://dev.azure.com/msr-dsail/LearnedLSM/_git/ycsb-runner)
#
# 3. You have filled out `experiment_config.sh` (see `README.md` for instructions).
#
# Usage: ./generate_ycsb_traces.sh path/to/the/ycsb-runner/repository/

set -e

function print_usage() {
  echo "Usage: $0 path/to/the/ycsb-runner/repository/"
}

function pushd () {
    command pushd "$@" > /dev/null
}

function popd () {
    command popd "$@" > /dev/null
}

function generate_load_trace() {
  local record_count=$1
  local trace_name=$2

  if [ -e $YCSB_TRACE_PATH/$trace_name ]; then
    >&2 echo "$trace_name already exists. Skipping."
    return
  fi

  ycsb load basic \
    -P ../workloads/workloada \
    -p recordcount=$record_count \
    | ./ycsbextractor $YCSB_TRACE_PATH/$trace_name
}

function generate_run_trace() {
  local workload=$1
  local record_count=$2
  local op_count=$3
  local trace_name=$4

  if [ -e $YCSB_TRACE_PATH/$trace_name ]; then
    >&2 echo "$trace_name already exists. Skipping."
    return
  fi

  ycsb run basic \
    -P ../workloads/workload$workload \
    -p recordcount=$record_count \
    -p operationcount=$op_count \
    | ./ycsbextractor $YCSB_TRACE_PATH/$trace_name
}

function build_and_generate_traces() {
  pushd $ycsb_runner_path
  mkdir -p build
  pushd build

  # Build the extractor first.
  cmake -DCMAKE_BUILD_TYPE=Release -DYR_BUILD_EXTRACTOR=ON ..
  make -j 8

  # Generate the traces.
  generate_load_trace 100000000 ycsb-100M-load
  generate_load_trace 20000000 ycsb-20M-load
  generate_run_trace a 100000000 10000000 ycsb-100M-10Mop-wa
  generate_run_trace a 20000000 10000000 ycsb-20M-10Mop-wa
  generate_run_trace c 100000000 10000000 ycsb-100M-10Mop-wc
  generate_run_trace c 20000000 10000000 ycsb-20M-10Mop-wc

  popd
  popd
}

function main() {
  if [ -z $1 ]; then
    print_usage
    exit 1
  fi

  if [ ! -d $1 ]; then
    echo "ERROR: The ycsb-runner repository could not be found. The provided path is not a directory."
    print_usage
    exit 1
  fi

  if [ -z $(which ycsb) ]; then
    echo "ERROR: The ycsb executable could not be found. Has YCSB been installed?"
    exit 1
  fi

  # Get an absolute path to the ycsb-runner repository.
  ycsb_runner_path=$(cd $1 && pwd -P)

  # Ensure the working directory is the same as the script's location.
  script_loc=$(cd $(dirname $0) && pwd -P)
  cd $script_loc

  # Load the config.
  source ../experiment_config.sh

  mkdir -p $YCSB_TRACE_PATH
  build_and_generate_traces
}

main $@
