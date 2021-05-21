# End-to-End Performance Benchmarks
This directory contains various scripts used to run a set of end-to-end
performance benchmarks on LLSM.

For more details about the experiments, see the wiki page
[here](https://dev.azure.com/msr-dsail/LearnedLSM/_wiki/wikis/LearnedLSM.wiki/3/Benchmarks).

## Getting Started
Follow the steps below to set up your environment for running the LLSM
experiments.

### Installing Conductor
The experiment pipeline is automated using a tool called Conductor. You need
to have it installed to be able to follow the later instructions in this
README. You can install Conductor using `pip`:

```
pip install --user conductor-cli
```

After installing Conductor, the `cond` executable should be available in your
`PATH`. Note that Conductor requires Python 3.8 or newer.

```
$ cond --version
Conductor 0.2.0
```

### Installing YCSB
To run the YCSB experiments, you either (i) need an installation of ycsb to
generate the workload traces, or (ii) need a copy of the workload traces.

You can install YCSB by following the instructions [here](https://ycsb.site).
Then to generate the YCSB traces, run the `generate_ycsb_traces.sh` script
(read the comments at the top of the script for more instructions.)

### Experiment Config
In the `script` top-level directory, make a copy of
`experiment_config_example.sh` and name it `experiment_config.sh`. Modify the
variables as needed (they are documented in the file).

## Running the Experiments

### Against a particular commit
Run `./run.sh commit` in your shell to run against the current commit. To run
against a specific commit, pass the commit hash to the script (a short hash
will work) (e.g., `./run.sh commit abcd123`).

### Run the CI job
Run `./run.sh ci` in your shell. The script will run the LLSM benchmarks
against `master`. The results will be emailed to
`llsm-dev@lists.csail.mit.edu`. Results (as well as the last commit against
which the experiments were run) are archived under the `results/`
subdirectory. This script will abort if the latest commit on `master` is the
same as (or older than) the last archived commit.

### Manually using Conductor
You can manually run parts of the experiment with Conductor by running
`cond run <task identifier>` with a specific task identifier. We list the
primary ones below.

- `//scripts/e2e_perf/synth_write:summarize_overall`
  - Runs the `synth_write` experiments and aggregates the results into one
    `.csv` file.
- `//scripts/e2e_perf/ycsb:summarize_overall`
  - Runs the `ycsb` experiments and aggregates the results into one `.csv`
    file.

All the experiment outputs will be stored under the `cond-out` directory (at
the root of this repository).
