#!/bin/bash

INPUT_FILE="C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\DATAFEED\\raw_dataset\\20240701_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
OUTPUT_FILE="C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\DATAFEED\\ficheros_tratados\\20240701_Equities_feed_IssuerLevel_sinOVR_vdpermid.csv.csv"
 
# Measure the time taken
START_TIME=$(date +%s)
 
# Process the file with awk to remove duplicates based on column 253
awk -F, '!seen[$253]++' "$INPUT_FILE" > "$OUTPUT_FILE"
 
# Measure the time taken
END_TIME=$(date +%s)
ELAPSED_TIME=$((END_TIME - START_TIME))
 
echo "Time taken: $ELAPSED_TIME seconds"