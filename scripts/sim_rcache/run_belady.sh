#! /bin/bash

workload=$1
shift 1

cd ../../simulator
python3 belady_caching.py ../scripts/sim_rcache/workloads/$workload $@ --print_csv > $COND_OUT/results.csv
