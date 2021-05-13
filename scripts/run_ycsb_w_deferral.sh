#! /bin/bash

########## Experiment Configuration ##########
WORKLOAD_PATH="/home/markakis/LearnedLSM/workloads"

declare -a WORKLOAD_PAIRS=(
  "-load_path=${WORKLOAD_PATH}/ycsb-20M-load -workload_path=${WORKLOAD_PATH}/ycsb-20M-20Mop-wa"
#  "-load_path=${WORKLOAD_PATH}/ycsb-20M-load -workload_path=${WORKLOAD_PATH}/ycsb-20M-10Mop-wa-updateonly"
)
declare -a THRES=(
  "1"
  "1600"
  "3200"
  "6400"
  "12800"
  "25600"
  "1 -deferral_autotuning=true"
 # "3200"
 # "6400"
)

declare -a DEFERS=(
  "1"
 # "2"
)

declare -a THREADS=(
  #"1"
  "4"
)

declare -a DIRECT_IO=(
 # "false"
  "true"
)

##############################################

NOW=$(date "+%F_%H_%M")

LOG_FILE=~/LearnedLSM/learnedlsm/bench/results/experiment_$NOW
STATS_FILE=${LOG_FILE}_stats
touch $LOG_FILE
touch $STATS_FILE
echo "${LOG_FILE}" > $LOG_FILE
echo "${STATS_FILE}" > $STATS_FILE

for workload in "${WORKLOAD_PAIRS[@]}"; do
  for threads in "${THREADS[@]}"; do
    for direct in "${DIRECT_IO[@]}"; do
      for threshold in "${THRES[@]}"; do
        for deferrals in "${DEFERS[@]}"; do
          name="${workload} -threads=${threads} -use_direct_io=${direct} -io_min_batch_size=${threshold} -max_deferrals=${deferrals}"
          echo "[$(date +"%D %T")] Running $name"
          rm -rf test
          ~/LearnedLSM/learnedlsm/build/release/bench/ycsb --db_path=test -db=llsm -cache_size_mib=256 -bg_threads=16 $name >> $LOG_FILE 2>> $STATS_FILE		
        done 
      done
    done
  done
done
