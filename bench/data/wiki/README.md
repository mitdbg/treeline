# Download Wikipedia edit timestamps
```
wget https://raw.githubusercontent.com/learnedsystems/SOSD/master/scripts/download.sh
./download.sh

Or copy it from tebow:/spinning/kipf/sosd_data/data/wiki_ts_200M_uint64
```

# Generate Wikipedia trace
```
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j genwiki
./genwiki wiki_ts_200M_uint64
```
