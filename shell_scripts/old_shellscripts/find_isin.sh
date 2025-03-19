#!/bin/bash

# Function to check if file exists
check_file_exists() {
    if [ ! -f "$1" ]; then
        echo "Error: File $2 does not exist."
        exit 1
    fi
}

# Input path
input_path="C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\DATAFEED\\raw_dataset\\20240701_Production\\20240701_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
file_name="${input_path##*\\}"

# Check if file exists
check_file_exists "$input_path" "$file_name"

# Prompt user for ISIN
read -p "Enter the ISIN code: " ISIN

# Trim leading and trailing spaces from ISIN
ISIN=$(echo "$ISIN" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

# Measure the time taken
START_TIME=$(date +%s)

# Search for ISIN in the CSV file (case-insensitive)
result=$(awk -F, -v IGNORECASE=1 -v isin="$ISIN" '$1 == isin {for(i=1; i<=10; i++) printf "%s,", $i; print ""; exit}' "$input_path")

# Check if ISIN was found
if [ -z "$result" ]; then
    echo "Sorry, the ISIN $ISIN is not there."
else
    echo "ISIN found. First 10 columns:"
    echo "$result" | sed 's/,/ /g'
fi

# Measure the time taken
END_TIME=$(date +%s)
ELAPSED_TIME=$((END_TIME - START_TIME))

echo "Time taken: $ELAPSED_TIME seconds"