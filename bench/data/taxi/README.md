# Download taxi data, extract location columns, and remove outliers
```
./scripts/download_year.sh 2015

Note that this dataset isn't sorted by time.
One way to sort it is to import it into DuckDB, sort it by the pickup timestamp column, and write it back to CSV.
https://duckdb.org/docs/data/csv

./scripts/extract_year.sh 2015

Extracts the lng/lat columns.
Produces yellow_tripdata_2015-01_lnglat.csv etc.

./scripts/clean_year.sh 2015
```

# Convert lat/lng coordinates to S2 cell ids (64-bit unsigned integers)
```
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j contaxi
./contaxi yellow_tripdata_2015-01_lnglat.csv
```
