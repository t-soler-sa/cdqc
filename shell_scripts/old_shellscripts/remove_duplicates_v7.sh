#!/bin/bash

# Define input and output files
# Local input file: 
INPUT_FILE="C:\Users\\n740789\\OneDrive - Santander Office 365\\Documentos\\Projects\DataSets\\DATAFEED\\raw_dataset\\20240301_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
# Local output file: 
OUTPUT_FILE="C:\\Users\\n740789\OneDrive - Santander Office 365\\Documentos\\Projects\\DataSets\\DATAFEED\\ficheros_tratados\\t_20240301_Equities_feed_IssuerLevel_sinOVR_v7.csv"
#INPUT_FILE="X:\\INVDESPRO\\INVESTIGACION\\Fondos éticos\\3. DATASETS\\04_Datos Clarity\\01_Equities_feed\\01_Ficheros_originales (Descarga en bruto)\\20240601_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
#OUTPUT_FILE="X:\\INVDESPRO\\INVESTIGACION\\Fondos éticos\\3. DATASETS\\04_Datos Clarity\\01_Equities_feed\\02_Ficheros_tratados\\2024\\output_tristan\\t_20240601_Equities_feed_IssuerLevel_sinOVR_v7.csv"

# Use awk to process the CSV and time the execution
start_time=$(date +%s)

#!/bin/bash

# Process the CSV: remove the first two columns and filter out duplicates 
awk -F, 'BEGIN {OFS=","} {
    $1=$2="";                     # Remove the first two columns
    line = substr($0, 3);         # Get the line starting from the third character to remove initial commas
    if (!seen[line]++) {          # Check if the line has been seen before
        print line                # Print only if this is the first occurrence
    }
}' "$INPUT_FILE" > "$OUTPUT_FILE"


end_time=$(date +%s)
execution_time=$((end_time - start_time))

echo "Execution time: $execution_time seconds"