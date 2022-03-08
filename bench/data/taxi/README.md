# Download taxi data, extract location columns, and remove outliers
```
./scripts/download_year.sh 2015
./scripts/extract_year.sh 2015
./scripts/clean_year.sh 2015

Produces yellow_tripdata_2015-01_lnglat.csv
```

# Convert lat/lng coordinates to S2 cell ids (64-bit unsigned integers)
```
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j contaxi
./contaxi yellow_tripdata_2015-01_lnglat.csv
```
