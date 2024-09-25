#!/bin/bash
 
# Define input and output file names

INPUT_FILE="C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\DATAFEED\raw_dataset\20240601_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
#OUTPUT_FILE="C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\DATAFEED\raw_dataset\20240201_Equities_redux.csv"
OUTPUT_FILE="X:\INVDESPRO\INVESTIGACION\Fondos éticos\3. DATASETS\04_Datos Clarity\01_Equities_feed\02_Ficheros_tratados\2024\output_tristan\t_20240401_Equities_feed_IssuerLevel_sinOVR.csv"


# Start the timer
START_TIME=$(date +%s)
 
# Remove duplicates based on the permId column, automatically find column index
awk -F, '
NR == 1 {
    for (i=1; i<=NF; i++) {
        if ($i == "permId") {
            permIdCol = i;
            break;
        }
    }
}
!seen[$permIdCol]++' "$INPUT_FILE" > "$OUTPUT_FILE"
 
# End the timer
END_TIME=$(date +%s)
 
# Calculate the duration in seconds
DURATION=$(( END_TIME - START_TIME ))
echo "Time taken: $DURATION seconds"