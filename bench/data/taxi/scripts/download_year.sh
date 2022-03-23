year=$1
for i in {1..9}; do
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$year-0$i.csv
done
for i in {10..12}; do
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$year-$i.csv
done
