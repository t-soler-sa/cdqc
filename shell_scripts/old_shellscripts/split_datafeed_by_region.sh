#!/bin/bash

# Check if USER_DATE is already set from environment
if [ -n "$USER_DATE" ]; then
    DATE=$USER_DATE
else
    # Ask for user input only if USER_DATE is not set
    read -p "Enter the date in YYYYMM format (press Enter for current month): " USER_INPUT
    # Use user input if provided, otherwise use current date
    if [ -z "$USER_INPUT" ]; then
        DATE=$(date +"%Y%m")
    else
        # Validate user input
        if [[ ! $USER_INPUT =~ ^[0-9]{6}$ ]]; then
            echo "Invalid date format. Please use YYYYMM."
            exit 1
        fi
        DATE=$USER_INPUT
    fi
fi

# Record the start time
start_time=$(date +%s)

DATE01="${DATE}01"

# Rest of your script continues here...

# Input and output directories
INPUT_FILE="/c/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/datafeeds_with_ovr/${DATE01}_datafeed_with_ovr.csv"
OUTPUT_DIR="/c/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/ficheros_tratados/Feed_region/${DATE}"

# Extract the year and month from the DATE variable
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

# Count the number of lines in each output file
echo "Lines in each output file:"
for region in "${allowed_regions[@]}"; do
    out_file="$OUTPUT_DIR/Equities_${region// /_}_${MONTH}${YEAR}.csv"
    line_count=$(wc -l < "$out_file")
    echo "$region: $line_count"
done

# Print how long it took to run
end_time=$(date +%s)
execution_time=$((end_time - start_time))

# Calculate minutes and seconds
minutes=$((execution_time / 60))
seconds=$((execution_time % 60))

echo "Execution time: ${minutes} minutes and ${seconds} seconds"