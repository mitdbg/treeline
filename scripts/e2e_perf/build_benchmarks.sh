#! /bin/bash
set -e

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc/../..

mkdir -p build && cd build

cmake -DCMAKE_BUILD_TYPE=Release -DLLSM_BUILD_BENCHMARKS=ON ..
make -j synth_write ycsb
