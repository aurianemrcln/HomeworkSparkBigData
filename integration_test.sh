#!/bin/bash

# # 1. Clean previous runs (Optional, to start fresh)
# rm -rf bal_latest
# rm -rf bal.db
# rm -rf bal_processing_buffer
# rm -rf bal_recomputed

# # 2. Run the loop for 10 days
# for n in $(seq 0 10); do 
#     day=$(date -d "2025-01-01 +${n} day" +%Y-%m-%d)
#     echo ""
#     echo "------------------------------------------------"
#     echo "Processing Day $((n+1)): ${day}"
    
#     # Input file path (must match where generate_data.sh put them)
#     INPUT_FILE="data_generated/dump-${day}.csv"
    
#     # Run Feature 1: Integration
#     ./run_daily_file_integration.sh "${day}" "${INPUT_FILE}"
    
#     # # Run Feature 2: Report
#     # ./run_report.sh
# done

# echo "------------------------------------------------"
# echo "Daily Integration Loop Finished."

# # 3. Test Feature 3: Time Travel (Recompute Dump)
# echo ""
# echo "Testing Time Travel..."
# for n in $(seq 0 10); do
#     date=$(date -d "2025-01-01 +${n} day" +%Y-%m-%d)
#     echo "Recomputing and extracting dump at date: ${date}"
#     ./recompute_and_extract_dump_at_date.sh ${date} $"bal_recomputed/day=${date}"
# done

# 4. Test Feature 4: Diff
dateA="2025-01-10"
dateB="2025-01-07"
echo "Comparing ${dateA} and ${dateB}..."
./compute_diff_between_files.sh "bal_recomputed/day=${dateA}" "bal_recomputed/day=${dateB}"