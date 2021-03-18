#! /bin/bash

########## Experiment Configuration ##########
WORKLOAD_PATH="../bench/workloads"

declare -a WORKLOAD_PAIRS=(
  "-load_path=${WORKLOAD_PATH}/ycsb-a-load-20m -workload_path=${WORKLOAD_PATH}/ycsb-a-run-20m-uniform"
  "-load_path=${WORKLOAD_PATH}/ycsb-a-load-20m -workload_path=${WORKLOAD_PATH}/ycsb-a-run-20m-zipfian"
  "-load_path=${WORKLOAD_PATH}/ycsb-a-load-100m -workload_path=${WORKLOAD_PATH}/ycsb-a-run-100m-uniform"
  "-load_path=${WORKLOAD_PATH}/ycsb-a-load-100m -workload_path=${WORKLOAD_PATH}/ycsb-a-run-100m-zipfian"
)
declare -a THRES=(
  "0"
  "1"
  "100"
  "200"
  "400"
  "800"
  "1600"
  "3200"
)

declare -a DEFERS=(
  "0"
  "1"
  "2"
  "3"
  "4"
)


##############################################

NOW=$(date "+%F_%H_%M")

LOG_FILE=results/experiment_$NOW
touch $LOG_FILE
echo "${LOG_FILE}" > $LOG_FILE

for workload in "${WORKLOAD_PAIRS[@]}"; do
  for threshold in "${THRES[@]}"; do
    for deferrals in "${DEFERS[@]}"; do
      name="${workload} -io_threshold=${threshold} -max_deferrals=${deferrals}"
      echo "[$(date +"%D %T")] Running $name"
      ~/LearnedLSM/learnedlsm/build/bench/deferred_io $name >> $LOG_FILE 					
    done 
  done
done
