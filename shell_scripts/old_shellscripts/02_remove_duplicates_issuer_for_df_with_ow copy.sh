#!/bin/bash

INPUT_FILE="C:/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/raw_dataset/20231001_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
OUTPUT_FILE="C:/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/ficheros_tratados/20231001_Equities_feed_IssuerLevel_sinOVR.csv"


# Start timing
start_time=$(date +%s)

# Use awk to remove duplicates based on the issuer_name column (index 3)
awk -F',' '
BEGIN { OFS = FS }
NR == 1 { print; next }
!seen[$3]++
' "$INPUT_FILE" > "$OUTPUT_FILE"

# End timing
end_time=$(date +%s)

# Calculate and display execution time
execution_time=$((end_time - start_time))
echo "Execution time: $execution_time seconds"

# Display the number of rows in the input and output files
input_rows=$(wc -l < "$INPUT_FILE")
output_rows=$(wc -l < "$OUTPUT_FILE")
echo "Input file rows: $input_rows"
echo "Output file rows: $output_rows"
echo "Rows removed: $((input_rows - output_rows))"