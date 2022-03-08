year=$1
for i in {1..9}; do
	./extract.sh yellow_tripdata_"$year"-0"$i".csv yellow_tripdata_"$year"-0"$i"_lnglat.csv &
done
for i in {10..12}; do
	./extract.sh yellow_tripdata_"$year"-"$i".csv yellow_tripdata_"$year"-"$i"_lnglat.csv &
done
wait
