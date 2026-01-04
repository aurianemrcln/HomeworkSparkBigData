#!/bin/bash

# 1. Clean previous runs (Optional, to start fresh)
rm -rf bal_latest
rm -rf bal.db

# 2. Run the loop for 50 days
for n in $(seq 1 50); do 
    day=$(date -d "2025-01-01 +${n} day" +%Y-%m-%d)
    
    echo "------------------------------------------------"
    echo "Processing Day ${n}: ${day}"
    
    # Input file path (must match where generate_data.sh put them)
    INPUT_FILE="C:/Users/auria/OneDrive - DVHE/Auriane/Ecole/ESILV/A5/S9/Spark for Big Data/Homework/HomeworkSparkBigData/data_generated/adresses-${day}.csv"
    
    # Run Feature 1: Integration
    ./run_daily_file_integration.sh "${day}" "${INPUT_FILE}"
    
    # Run Feature 2: Report
    ./run_report.sh
done

echo "------------------------------------------------"
echo "Daily Integration Loop Finished."

# 3. Test Feature 3: Time Travel (Recompute Dump)
echo "Testing Time Travel..."
# Reconstruct state as of Jan 24th (Should be 'Rue de Paris')
./recompute_and_extract_dump_at_date.sh "2025-01-24" "C:/temp/dumpA"

# Reconstruct state as of Feb 10th (Should be 'Avenue des Champs' due to change on Day 25)
./recompute_and_extract_dump_at_date.sh "2025-02-10" "C:/temp/dumpB"

# 4. Test Feature 4: Diff
echo "Comparing Dump A and Dump B..."
./compute_diff_between_files.sh "C:/temp/dumpA" "C:/temp/dumpB"