#!/bin/bash

# Input and output file paths
input_file="/c/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/ficheros_tratados/joined_output.csv"
output_file="${input_file%.*}_deduplicated.csv"
temp_file="${input_file%.*}_temp.csv"

# Check if the input file exists
if [ ! -f "$input_file" ]; then
    echo "Error: Input file not found!"
    exit 1
fi

# Start the timer
start_time=$(date +%s)

# Extract the header (first line) and the rest of the file
head -n 1 "$input_file" > "$output_file"
tail -n +2 "$input_file" > "$temp_file"

# Sort and remove duplicates from the data, then append to the output file
sort -u "$temp_file" >> "$output_file"

# Remove the temporary file
rm "$temp_file"

# End the timer
end_time=$(date +%s)

# Calculate the duration
duration=$((end_time - start_time))

# Check if the operation was successful
if [ $? -eq 0 ]; then
    echo "Duplicate rows removed successfully."
    echo "Output file: $output_file"
    echo "Original row count: $(wc -l < "$input_file")"
    echo "New row count: $(wc -l < "$output_file")"
    echo "Time taken: $duration seconds"
else
    echo "Error: Failed to remove duplicate rows."
    exit 1
fi