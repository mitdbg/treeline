#! /bin/bash

set -e

################### Usage ####################

# Run experiments on a specific commit
# Usage: ./run.sh commit [git commit hash]
# If the git hash is not provided, this script will run all the experiments on
# the current commit. The summarized results will be copied into the `results`
# subdirectory.

# CI job for performance validation
# Usage: ./run.sh ci
# This will run the experiments only if there has been new commits made to the
# repository. The experiment results will be sent via email. The summarized
# results will also be copied into the `results` subdirectory.

############ Configuration Options ###########

# NOTE: Generally, you do not need to change any of these options.

# Processed results will be saved in this directory.
RESULTS_DIR="results"

# The last processed git hash will be stored in this file. This is used to
# determine whether to run the experiments again.
LAST_COMMIT_HASH_FILE="last_run_hash"

# The top-level Conductor tasks to run.
COND_LLSM_TASKS="//scripts/e2e_perf:llsm"
COND_ROCKSDB_TASKS="//scripts/e2e_perf:rocksdb"
COND_SUMMARIZE_TASK="//scripts/e2e_perf:summarize_overall"

# The path to the combined results file, relative to `cond-out`.
COND_RESULTS_FILE="scripts/e2e_perf/summarize_overall.task/results.csv"

##############################################

# Ensure the working directory is the same as the script's location.
script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc

function git_hash() {
  echo "$(git rev-parse HEAD)"
}

function git_short_hash() {
  echo "$(git rev-parse --short HEAD)"
}

function git_is_ancestor() {
  git merge-base --is-ancestor $current_hash $last_run_hash > /dev/null 2>&1
  echo $?
}

function run_experiments() {
  # Run the RocksDB experiments first. If the experiment results already exist,
  # Conductor will not run them again.
  cond run $COND_ROCKSDB_TASKS

  # Run the LLSM experiments next. Using --again will force Conductor to re-run
  # the experiments, even if older results exist.
  cond run $COND_LLSM_TASKS --again

  # Group the results together into one CSV file.
  cond run $COND_SUMMARIZE_TASK

  local short_hash=$(git_short_hash)
  local timestamp=$(date +%s)
  local results_name="${short_hash}_${timestamp}.csv"

  # The location of the results will be stored in $results_path
  results_path=$RESULTS_DIR/$results_name

  # Copy the results to a local archive directory.
  cp ../../cond-out/$COND_RESULTS_FILE $results_path
}

function ci_job() {
  # The CI job only runs against master.
  git checkout master
  git pull origin master

  local last_run_hash=$(cat $RESULTS_DIR/$LAST_COMMIT_HASH_FILE)
  local current_hash=$(git_hash)
  if [ $(git_is_ancestor) -eq 0 ]; then
    # The current commit hash is an ancestor of the last run's commit hash.
    # This means either the last run was against a commit that came after the
    # current commit (or was against the same commit). Since we want to only
    # re-run against newer code, we abort here.
    >&2 echo "No new additions to the codebase since the last run. Aborting."
    exit 0
  fi

  # Run the experiments. The results will be in a file at $results_path.
  run_experiments

  # Record the current hash to avoid re-running the experiments if the code
  # does not change.
  echo $current_hash > $RESULTS_DIR/$LAST_COMMIT_HASH_FILE

  >&2 echo "Experiments done! Results saved as $results_path"
  >&2 echo "Sending email notification now..."

  # Run the email notification script.
  python3 send_results_email.py --result $results_path

  >&2 echo "Done!"
}

function commit_job() {
  local commit_hash=$1
  if [ ! -z "$commit_hash" ]; then
    local initial_branch="$(git symbolic-ref --short HEAD)"
    git checkout $commit_hash
  fi

  >&2 echo "Running experiments against commit $(git_hash)."
  run_experiments
  >&2 echo "Done! Results saved as $results_path"

  if [ ! -z "$initial_branch" ]; then
    git checkout "$current_branch"
  fi
}

function print_usage() {
  >&2 echo "Usage:           $0 <commit|ci>"
  >&2 echo "Usage (commit):  $0 commit [git commit hash]"
  >&2 echo "Usage (ci):      $0 ci"
}

function main() {
  if [ -z "$1" ]; then
    print_usage $@
    exit 1
  fi
  job=$1
  shift

  # Make sure Conductor is available.
  if [ -z "$(which cond)" ]; then
    >&2 echo "ERROR: Conductor was not found. To install Conductor, run"
    >&2 echo "pip install --user conductor-cli."
    exit 1
  fi

  # Prepare the environment for running the experiments.
  mkdir -p $RESULTS_DIR
  touch $RESULTS_DIR/$LAST_COMMIT_HASH_FILE

  if [ "$job" == "ci" ]; then
    ci_job $@
  elif [ "$job" == "commit" ]; then
    commit_job $@
  else
    >&2 echo "Unrecognized job: $job"
    print_usage $@
    exit 1
  fi
}

main $@
