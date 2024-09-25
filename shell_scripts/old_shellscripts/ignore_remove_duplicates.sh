#!/bin/bash
 
# Define input and output file names
INPUT_FILE="X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/01_Ficheros_originales (Descarga en bruto)/20240401_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
OUTPUT_FILE="X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/01_Ficheros_originales_redux.csv"
 
# Start the timer
START_TIME=$(date +%s)
 
# Sort the CSV by the permId column and remove duplicates
csvsort -c permId "$INPUT_FILE" | csvdedupe -c permId > "$OUTPUT_FILE"
 
# End the timer
END_TIME=$(date +%s)
 
# Calculate the duration in minutes
DURATION=$(( (END_TIME - START_TIME) / 60 ))
 
# Print the time taken
echo "Time taken: $DURATION minutes"