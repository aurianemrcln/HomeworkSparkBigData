#!/bin/bash
# Usage: ./run_report.sh

spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  target/spark-bal-project-1.0-SNAPSHOT.jar \
  "REPORT"