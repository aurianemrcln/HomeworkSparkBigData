#!/bin/bash

# Create a base data directory (using Linux style path for Git Bash)
# /c/temp/data maps to C:\temp\data on Windows
BASE_DIR="data_generated"
mkdir -p "$BASE_DIR"

echo "Generating dummy data in $BASE_DIR..."

# Create specific folder for that day
mkdir -p "$BASE_DIR"

# Loop 50 days
for n in $(seq 0 50); do
    # Calculate date
    day=$(date -d "2025-01-01 +${n} day" +%Y-%m-%d)
    
    
    
    # Create a CSV file
    # We make a small change on Day 25 to test 'Updates'
    if [ $n -eq 25 ]; then
        # Day 25: Same ID (1), but address changes to 'Avenue des Champs'
        echo "id;numero;voie;code_postal;commune" > "$BASE_DIR/adresses-${day}.csv"
        echo "1;10;Avenue des Champs;75000;Paris" >> "$BASE_DIR/adresses-${day}.csv"
    else
        # Normal Days: Original address
        echo "id;numero;voie;code_postal;commune" > "$BASE_DIR/adresses-${day}.csv"
        echo "1;10;Rue de Paris;75000;Paris" >> "$BASE_DIR/adresses-${day}.csv"
    fi
done

echo "Data generation complete."