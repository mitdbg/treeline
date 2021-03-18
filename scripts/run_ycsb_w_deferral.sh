#! /bin/bash

########## Experiment Configuration ##########
WORKLOAD_PATH="/home/markakis/LearnedLSM/learnedlsm/bench/workloads"

declare -a WORKLOAD_PAIRS=(
  "-load_path=${WORKLOAD_PATH}/ycsb-20M-load -workload_path=${WORKLOAD_PATH}/ycsb-20M-20Mop-wa"
#  "-load_path=${WORKLOAD_PATH}/ycsb-20M-load -workload_path=${WORKLOAD_PATH}/ycsb-20M-10Mop-wa-updateonly"
)
declare -a THRES=(
  "1"
  "100"
  "200"
  "400"
  "800"
  "1600"
  "3200"
  "6400"
)

declare -a DEFERS=(
  "1"
  "2"
)

declare -a THREADS=(
  "1"
  "4"
)

declare -a DIRECT_IO=(
  "false"
  "true"
)

##############################################

NOW=$(date "+%F_%H_%M")

LOG_FILE=~/LearnedLSM/learnedlsm/bench/results/experiment_$NOW
touch $LOG_FILE
echo "${LOG_FILE}" > $LOG_FILE

for workload in "${WORKLOAD_PAIRS[@]}"; do
  for threads in "${THREADS[@]}"; do
    for direct in "${DIRECT_IO[@]}"; do
      for threshold in "${THRES[@]}"; do
        for deferrals in "${DEFERS[@]}"; do
          name="${workload} -threads=${threads} -use_direct_io=${direct} -io_threshold=${threshold} -max_deferrals=${deferrals}"
          echo "[$(date +"%D %T")] Running $name"
          rm -rf test
          ~/LearnedLSM/learnedlsm/build/release/bench/ycsb --db_path=test -db=llsm -cache_size_mib=256 -bg_threads=16 $name >> $LOG_FILE 		
        done 
      done
    done
  done
done
