#!/bin/bash

# start timer
start_time=$(date +%s)

# Set the input file and  ouput folder
input_file="C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\DATAFEED\\raw_dataset\\20240701_Production\\20240701_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
ouput_folder="C:\\Users\\n740789\\OneDrive - Santander Office 365\\Documentos\\Projects\\DataSets\\DATAFEED\\ficheros_tratados\\Feed_region"

# Extract the date from the input file name
date=$(basename "$input_file" | grep -oE "[0-9]{8}" | head -1)
year=${date:0:4}
month=${date:4:2}

# Process the csv file and generate multiple output files based on the region column
awk -v year=$"year" -v month="$month" -v output_folder="$output_folder" '
BEGIN {FS = ","; OFS = ","}
{
    region = $6
    outfile = "Equities_" region "_" month year ".csv"
    print > output_folder "/" outfile
}' "$input_file"
 
# Print how long it took to run
echo "Execution time: $execution_time seconds"