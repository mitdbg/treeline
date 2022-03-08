year=$1
for i in {1..9}; do
	./clean.sh yellow_tripdata_"$year"-0"$i"_lnglat.csv &
done
for i in {10..12}; do
	./clean.sh yellow_tripdata_"$year"-"$i"_lnglat.csv &
done
wait
