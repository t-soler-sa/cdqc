#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status.

# Start the timer
start_time=$(date +%s)

main_file="C:/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/raw_dataset/20240801_Equities_feed_new_strategies_filtered_old_names_iso_permId_all.csv"
cross_ref_file="C:/Users/n740789/Documents/Projects_local/DataSets/crossreference/Aladdin_Clarity_Issuers_20240701.csv"
output_dir="C:/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/ficheros_tratados"
output_file="$output_dir/joined_output.csv"

# Function to convert Windows path to Unix-style
convert_path() {
    echo "$1" | sed 's/\\/\//g' | sed 's/C:/\/c/'
}

main_file=$(convert_path "$main_file")
cross_ref_file=$(convert_path "$cross_ref_file")
output_dir=$(convert_path "$output_dir")
output_file=$(convert_path "$output_file")

# Check if files and directories exist
for file in "$main_file" "$cross_ref_file"; do
    if [ ! -f "$file" ]; then
        echo "Error: File $file does not exist."
        exit 1
    fi
done

# Ensure output directory exists
if [ ! -d "$output_dir" ]; then
    echo "Creating output directory: $output_dir"
    mkdir -p "$output_dir" || { echo "Error: Unable to create output directory."; exit 1; }
fi

# Join the files and process the data
echo "Processing files..."
awk -F',' '
BEGIN {
    OFS=","
    RS="\n"
}
# Process cross-reference file
NR==FNR {
    if (FNR == 1) {
        cross_ref_headers = $1 "," $2
        main_header = "Main_Column_3"
    } else {
        cross_ref[$3] = $1 "," $2
        cross_ref_keys[$3] = 1
    }
    next
}
# Process main file
FNR == 1 {
    main_header = $3
    print main_header ",permid," cross_ref_headers ",JoinStatus"
    next
}
{
    # Clean up column 3: remove commas and replace newlines with spaces
    gsub(",", "", $3)
    gsub(/\n/, " ", $3)
    
    if ($253 in cross_ref) {
        print $3 "," $253 "," cross_ref[$253] ",Matched"
        delete cross_ref_keys[$253]
    } else {
        print $3 "," $253 ",NA,NA,Unmatched_Main"
    }
}
END {
    for (key in cross_ref_keys) {
        print "missing_data," key "," cross_ref[key] ",Unmatched_CrossRef"
    }
}
' "$cross_ref_file" "$main_file" > "$output_file" || { echo "Error during file processing."; exit 1; }

# Count and display statistics
echo "Calculating statistics..."
matched=$(grep -c ",Matched" "$output_file") || matched=0
unmatched_main=$(grep -c ",Unmatched_Main" "$output_file") || unmatched_main=0
unmatched_cross=$(grep -c ",Unmatched_CrossRef" "$output_file") || unmatched_cross=0
total=$((matched + unmatched_main + unmatched_cross))

echo "Join Statistics:"
echo "Matched rows: $matched"
echo "Unmatched rows from main file: $unmatched_main"
echo "Unmatched rows from cross-reference file: $unmatched_cross"
echo "Total rows: $total"

echo "Output saved to $output_file"

# Calculate and display execution time
end_time=$(date +%s)
execution_time=$((end_time - start_time))
hours=$((execution_time / 3600))
minutes=$(( (execution_time % 3600) / 60 ))
seconds=$((execution_time % 60))

echo "Execution time: $hours hours, $minutes minutes, $seconds seconds"

# Calculate and display processing rate
if command -v bc &> /dev/null; then
    rate=$(echo "scale=2; $total / $execution_time" | bc)
    echo "Processing rate: $rate rows per second"
else
    echo "Note: 'bc' command not found. Unable to calculate processing rate."
fi