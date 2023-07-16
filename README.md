![](https://github.com/mitdbg/learnedlsm/actions/workflows/ci.yml/badge.svg)

# TreeLine
An embedded key-value store for modern SSDs.

## Building from source

### Install Dependencies

A few packages that Treeline depends on are

- `libtbb-dev`
- `autoconf`
- `libjemalloc-dev`

On Ubuntu distribution the dependency installation command would look something like this

```
sudo apt-get install libtbb-dev autoconf libjemalloc-dev
```

Depending on the distro you have, ensure the above packages are installed. In case you do not
find the above packages, run the CMake, and fix as and when it throws errors.

CMake 3.17+ is required for building this project.

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
```

To build the tests, turn on the `TL_BUILD_TESTS` option when configuring.
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DTL_BUILD_TESTS=ON .. && make -j
```

To build the benchmarks, turn on the `TL_BUILD_BENCHMARKS` option when
configuring.
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DTL_BUILD_BENCHMARKS=ON .. && make -j
```
