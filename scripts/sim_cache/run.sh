#! /bin/bash

workload=$1
shift 1

cd ../../simulator
# We use the same workloads as the deferral simulation
python3 caching.py ../scripts/sim_deferral/workloads/$workload $@
