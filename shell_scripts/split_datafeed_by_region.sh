#!/bin/bash

# Start timer
start_time=$(date +%s)

# Input and output directories
INPUT_FILE="/c/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/datafeeds_with_ovr/20241001_datafeed_with_ovr.csv"
OUTPUT_DIR="/c/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/ficheros_tratados/Feed_region/202410"

# Extract the date from the input file name (format: YYYYMMDD)
FILE_NAME=$(basename "$INPUT_FILE")
DATE=$(echo "$FILE_NAME" | grep -oP '\d{8}')
YEAR=${DATE:0:4}
MONTH=${DATE:4:2}

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Define the allowed regions
allowed_regions=("N America" "Europe" "Asia Pacific" "Latam" "Emerging Markets")

# Use awk to split the file by the "region" column (index 6)
awk -F, -v year="$YEAR" -v month="$MONTH" -v output_dir="$OUTPUT_DIR" '
BEGIN {
    allowed_regions["N America"] = 1
    allowed_regions["Europe"] = 1
    allowed_regions["Asia Pacific"] = 1
    allowed_regions["Latam"] = 1
    allowed_regions["Emerging Markets"] = 1
}
NR == 1 {
    header = $0
    for (region in allowed_regions) {
        out_file = output_dir "/Equities_" region "_" month year ".csv"
        print header > out_file
    }
    next
}
{
    region = $6
    if (region in allowed_regions) {
        out_file = output_dir "/Equities_" region "_" month year ".csv"
        print >> out_file
    }
}' "$INPUT_FILE"

# Print how long it took to run
end_time=$(date +%s)
execution_time=$((end_time - start_time))
echo "Execution time: $execution_time seconds"

# Count the number of lines in each output file
echo "Lines in each output file:"
for region in "${allowed_regions[@]}"; do
    out_file="$OUTPUT_DIR/Equities_${region// /_}_${MONTH}${YEAR}.csv"
    line_count=$(wc -l < "$out_file")
    echo "$region: $line_count"
done