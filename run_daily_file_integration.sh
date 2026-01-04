#!/bin/bash
# Usage: ./run_daily_file_integration.sh <YYYY-MM-DD> <path_to_csv>

DATE=$1
CSV_FILE=$2

spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  target/spark-bal-project-1.0-SNAPSHOT.jar \
  "INTEGRATION" "$DATE" "$CSV_FILE"