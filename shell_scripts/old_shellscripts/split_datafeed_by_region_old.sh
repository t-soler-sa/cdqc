#!/bin/bash

# start timer
start_time=$(date +%s)


# Input and output directories
INPUT_PATH="/c/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/datafeeds_with_ow/20240701_datafeed_with_ow.csv"
OUTPUT_DIR="/c/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/ficheros_tratados/Feed_region/202407"

# Extract the date from the input file name (format: YYYYMMDD)
FILE_NAME=$(basename "$INPUT_FILE")
DATE=$(echo "$FILE_NAME" | grep -oP '\d{8}')
YEAR=${DATE:0:4}
MONTH=${DATE:4:2}

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Define the allowed regions as a string
allowed_regions="N America|Europe|Asia Pacific|Latam|Emerging Markets"

# Use awk to split the file by the "region" column (index 6)
awk -F, -v year="$YEAR" -v month="$MONTH" -v output_dir="$OUTPUT_DIR" -v allowed_regions="$allowed_regions" '
BEGIN {
    split(allowed_regions, regions, "|");
    for (i in regions) {
        allowed[regions[i]] = 1;
    }
}
NR == 1 { header = $0; next }
{
    region = $6
    if (region in allowed) {
        out_file = output_dir "/Equities_" region "_" month year ".csv"
        if (! (out_file in files)) {
            files[out_file] = 1
            print header > out_file
        }
        print >> out_file
    }
}' "$INPUT_FILE"

# Print how long it took to run
end_time=$(date +%s)
execution_time=$((end_time - start_time))
echo "Execution time: $execution_time seconds"