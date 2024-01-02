![](https://github.com/mitdbg/learnedlsm/actions/workflows/ci.yml/badge.svg)

# TreeLine
An embedded key-value store for modern SSDs.

## Building from source

### Install Dependencies

A few packages that TreeLine depends on are

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

## Inspecting the codebase

If you would like to read more about the internals of TreeLine, you can start at [this header file](https://github.com/mitdbg/treeline/blob/master/include/treeline/pg_db.h).

The bulk of the code that comprises the current version of the system can be found in the [`page_grouping/`](https://github.com/mitdbg/treeline/tree/master/page_grouping) directory.

Thank you for your interest in diving deeper in our work!


