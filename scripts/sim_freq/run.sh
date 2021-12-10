#! /bin/bash

workload=$1
shift 1

cd ../../simulator
python3 frequency_cluster.py ../scripts/sim_freq/workloads/$workload $@ > $COND_OUT/results.csv
