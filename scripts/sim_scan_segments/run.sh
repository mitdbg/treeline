#! /bin/bash

workload=$1
shift 1

cd ../../simulator
python3 scan_grouping.py ../scripts/sim_scan_segments/workloads/$workload $@
