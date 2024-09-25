#!/bin/bash

INPUT_FILE="C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ow\202407_df_issuer_level_with_ow.csv"
OUTPUT_FILE="C:\Users\n740789\Documents\Projects_local\DataSets\datafeed_issuerlevel_with_ow_structure_analysis.txt"

# Function to convert Windows path to Unix-style path
win_to_unix_path() {
    echo "$1" | sed 's/\\/\//g' | sed 's/C:/\/c/'
}

# Convert paths
UNIX_INPUT_FILE=$(win_to_unix_path "$INPUT_FILE")
UNIX_OUTPUT_FILE=$(win_to_unix_path "$OUTPUT_FILE")

# Use awk to analyze the CSV structure
awk -F',' '
BEGIN {
    print "CSV Structure Analysis:" > "'$UNIX_OUTPUT_FILE'"
}
NR == 1 {
    print "Number of fields detected: " NF >> "'$UNIX_OUTPUT_FILE'"
    print "First 10 characters of the line: " substr($0, 1, 10) >> "'$UNIX_OUTPUT_FILE'"
    print "\nDetected columns:" >> "'$UNIX_OUTPUT_FILE'"
    for (i = 1; i <= NF; i++) {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)  # Trim whitespace
        print i ": " $i >> "'$UNIX_OUTPUT_FILE'"
    }
    exit 0
}
' "$UNIX_INPUT_FILE"

echo "CSV structure analysis written to $OUTPUT_FILE"

# Check file encoding
file_encoding=$(file -i "$UNIX_INPUT_FILE" | awk -F"=" '{print $2}')
echo "File encoding: $file_encoding" >> "$UNIX_OUTPUT_FILE"

# Display the first few lines of the file
echo -e "\nFirst 5 lines of the file:" >> "$UNIX_OUTPUT_FILE"
head -n 5 "$UNIX_INPUT_FILE" >> "$UNIX_OUTPUT_FILE"

# Count number of columns based on the first line
num_columns=$(head -n 1 "$UNIX_INPUT_FILE" | awk -F',' '{print NF}')
num_rows=$(wc -l < "$UNIX_INPUT_FILE")

echo -e "\nAnalysis Summary:" >> "$UNIX_OUTPUT_FILE"
echo "Number of columns (based on first line): $num_columns" >> "$UNIX_OUTPUT_FILE"
echo "Number of rows: $num_rows" >> "$UNIX_OUTPUT_FILE"

echo "Analysis complete. Results written to $OUTPUT_FILE"I