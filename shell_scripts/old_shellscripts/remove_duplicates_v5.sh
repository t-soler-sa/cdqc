#!/bin/bash

# Define input and output files
# Local input file: 
INPUT_FILE="C:\\Users\\n740789\\OneDrive - Santander Office 365\\Documentos\\Projects\DataSets\\DATAFEED\\raw_dataset\\20240501_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
# Local output file: 
OUTPUT_FILE="C:\\Users\\n740789\OneDrive - Santander Office 365\\Documentos\\Projects\\DataSets\\DATAFEED\\ficheros_tratados\\t_20240501_Equities_feed_IssuerLevel_sinOVR_v5.csv"

#INPUT_FILE="X:\\INVDESPRO\\INVESTIGACION\\Fondos éticos\\3. DATASETS\\04_Datos Clarity\\01_Equities_feed\\01_Ficheros_originales (Descarga en bruto)\\20240501_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
#OUTPUT_FILE="X:\\INVDESPRO\\INVESTIGACION\\Fondos éticos\\3. DATASETS\\04_Datos Clarity\\01_Equities_feed\\02_Ficheros_tratados\\2024\\output_tristan\\t_20240501_Equities_feed_IssuerLevel_sinOVR_v5.csv"

# Use awk to process the CSV and time the execution
start_time=$(date +%s)

awk -F',' '{
    # Construct a row without the first two columns
    new_row = $3;
    for (i = 4; i <= NF; i++) {
        new_row = new_row "," $i;
    }
    # Add row to an array if not already present to eliminate duplicates
    if (!(new_row in seen)) {
        seen[new_row] = 1;
        print new_row;
    }
}' "$INPUT_FILE" > "$OUTPUT_FILE"

end_time=$(date +%s)
execution_time=$((end_time - start_time))

echo "Execution time: $execution_time seconds"