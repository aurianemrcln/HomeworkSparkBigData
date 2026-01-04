#!/bin/bash
# Usage: ./recompute_and_extract_dump_at_date.sh <YYYY-MM-DD> <output_dir>

DATE=$1
OUTPUT_DIR=$2

spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  target/spark-bal-project-1.0-SNAPSHOT.jar \
  "RECOMPUTE" "$DATE" "$OUTPUT_DIR"