#! /bin/bash

workload=$1
shift 1

cd ../../simulator
python3 deferral.py ../scripts/sim_deferral/workloads/$workload $@
