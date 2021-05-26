# The variables in this file are used when running LLSM's automated
# experiments. Their values are specific to the environment where you run the
# experiments, so you need to set them explicitly.
#
# To set the variables, make a copy of this configuration file and name it
# `experiment_config.sh`. Then, set the variables below in that file as needed
# for your own experimental setup. Note that the paths should be absolute.

# Path where the YCSB traces are stored.
YCSB_TRACE_PATH=/home/$USER/datasets

# Path where preloaded databases should be stored.
DB_CHECKPOINT_PATH=/flash1/$USER/llsm-checkpoint

# Path where the database(s) should be stored. This path will dictate the
# underlying storage device that is used to store the database, which will
# affect the experimental results.
DB_PATH=/flash1/$USER/llsm

# The PRNG seed to use for all experiments (used to ensure reproducibility).
SEED=42

# Path to third-party datasets.
TP_DATASET_PATH=/home/$USER/datasets
