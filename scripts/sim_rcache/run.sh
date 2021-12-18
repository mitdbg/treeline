#! /bin/bash

workload=$1
shift 1

cd ../../simulator
python3 record_caching.py ../scripts/sim_rcache/workloads/$workload $@ --out_dir=$COND_OUT
