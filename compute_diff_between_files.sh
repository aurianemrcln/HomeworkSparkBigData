#!/bin/bash
# Usage: ./compute_diff_between_files.sh <dir1> <dir2>

DIR1=$1
DIR2=$2

spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  target/spark-bal-project-1.0-SNAPSHOT.jar \
  "DIFF" "$DIR1" "$DIR2"