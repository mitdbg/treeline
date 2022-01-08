#! /bin/bash

dataset=$1
shift 1

# The simulation script expects different flags depending on the kind of dataset.
if [[ "$dataset" =~ .*\.yml$ ]]; then
  # Workload generator - file ends in .yml
  workload_arg="--workload_config=../scripts/sim_static_grouping/${dataset}"
else
  # Text file with keys on their own lines
  workload_arg="--custom_dataset=${dataset}"
fi

cd ../../simulator
python3 sequential_merge.py $workload_arg $@
