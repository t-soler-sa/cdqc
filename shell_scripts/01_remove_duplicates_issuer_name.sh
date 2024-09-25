#!/bin/bash
echo v7

INPUT_FILE="c:\Users\n740789\Downloads\Equities_feed_new_strategies_filtered_old_names_iso_permId_SIMULATION_v7.csv"
OUTPUT_FILE="c:\Users\n740789\Downloads\Equities_feed_new_strategies_filtered_old_names_iso_permId_SIMULATION_v7_issuer_level.csv"


# Start timing
start_time=$(date +%s)

# Use awk to remove duplicates based on the permId column (index 253)
awk -F',' '
BEGIN { OFS = FS }
NR == 1 { print; next }
!seen[$253]++
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