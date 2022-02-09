#! /bin/bash

# Record physical I/O activity.
iostat -o JSON -d -y 1 >$COND_OUT/iostat.json &
iostat_pid=$!

../../../build/page_grouping/pg_read2 --out_path=$COND_OUT $@

kill -s SIGINT -- $iostat_pid
wait
