#!/usr/bin/env bash
# Downloads the 2019 NYC Yellow Taxi trip files (about 1.2 GB) and the taxi zone lookup table.
set -e

DATA_DIR="$HOME/spark-data/a12"
BASE_URL="https://d37ci6vzurychx.cloudfront.net"

mkdir -p "$DATA_DIR/trips"

for m in 01 02 03 04 05 06 07 08 09 10 11 12
do
  FILE="yellow_tripdata_2019-$m.parquet"
  if [ -f "$DATA_DIR/trips/$FILE" ]; then
    echo "Already downloaded: $FILE"
  else
    curl -L -f -o "$DATA_DIR/trips/$FILE" "$BASE_URL/trip-data/$FILE"
  fi
done

curl -L -f -o "$DATA_DIR/taxi_zone_lookup.csv" "$BASE_URL/misc/taxi_zone_lookup.csv"

du -sh "$DATA_DIR/trips"
echo "Data is ready in $DATA_DIR"
