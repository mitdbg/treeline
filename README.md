![](https://github.com/mitdbg/learnedlsm/actions/workflows/ci.yml/badge.svg)

# TreeLine
An embedded key-value store for modern SSDs.

## Building from source

### Install Dependencies

A few packages that Treeline depends on are

- `libtbb-dev`
- `autoconf`
- `libjemalloc-dev`

Depending on the distribution you have, ensure the above packages are installed.
On Ubuntu, you can install the dependencies using `apt`:

```
sudo apt install libtbb-dev autoconf libjemalloc-dev
```

TreeLine's other dependencies are fetched by CMake during compilation.

### Compile

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
