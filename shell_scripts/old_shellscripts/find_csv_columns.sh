#!/bin/bash

# Hardcode the CSV file path
csv_file="C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\raw_dataset\20240901_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"

# Specify the columns we're interested in
columns=("isin" "permId" "issuer_name" "str_005_ec")

# Read the header of the CSV file
header=$(head -n 1 "$csv_file")

# Find the index of each column
for column in "${columns[@]}"; do
    index=$(echo "$header" | awk -F',' -v col="$column" '{
        for(i=1; i<=NF; i++) {
            if($i == col) {
                print i
                exit
            }
        }
    }')
    if [ -n "$index" ]; then
        echo "$column: $index"
    else
        echo "$column: not found"
    fi
done