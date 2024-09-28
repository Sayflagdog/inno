#!/bin/bash

while ! nc -z clickhouse 9000; do
  echo "Waiting for Clickhouse to be available..."
  sleep 2
done

python3 /app/data_processing.py
python3 /app/app.py



