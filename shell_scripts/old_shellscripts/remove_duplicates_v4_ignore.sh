#!/bin/bash

# Define input and output file names
INPUT_FILE="X:\INVDESPRO\INVESTIGACION\Fondos éticos\3. DATASETS\04_Datos Clarity\01_Equities_feed\01_Ficheros_originales (Descarga en bruto)\20240301_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
OUTPUT_FILE="X:\INVDESPRO\INVESTIGACION\Fondos éticos\3. DATASETS\04_Datos Clarity\01_Equities_feed\02_Ficheros_tratados\2024\output_tristan\t_20240301_Equities_feed_IssuerLevel_sinOVR_v3_2.csv"
# Start the timer
START_TIME=$(date +%s)

# Extract header line to get column indices for "isin" and "instrument_type"
header=$(head -n 1 "$INPUT_FILE")
IFS=',' read -r -a columns <<< "$header"

# Find the indices of "isin" and "instrument_type"
isin_idx=-1
instrument_type_idx=-1

for i in "${!columns[@]}"; do
  if [[ "${columns[$i]}" == "isin" ]]; then
    isin_idx=$i
  elif [[ "${columns[$i]}" == "instrument_type" ]]; then
    instrument_type_idx=$i
  fi
done

# Create an array of column indices to keep
keep_indices=()
for i in "${!columns[@]}"; do
  if [ $i != $isin_idx ] && [ $i != $instrument_type_idx ]; then
    keep_indices+=($i)
  fi
done

# Adjust indices for awk (1-based)
for ((i=0; i<${#keep_indices[@]}; i++)); do
  keep_indices[$i]=$((${keep_indices[$i]} + 1))
done

# Use awk to process the file
awk -v keep_indices="${keep_indices[*]}" '
BEGIN {FS=OFS=","}
{
  new_line = ""
  for(i in keep_indices) {
    new_line = new_line $i OFS
  }
  new_line = substr(new_line, 1, length(new_line)-1)
  if (!seen[new_line]++) {
    print new_line
  }
}
' "$INPUT_FILE" > "$OUTPUT_FILE"


# End the timer
END_TIME=$(date +%s)

# Calculate the duration in seconds
DURATION=$(( END_TIME - START_TIME ))
echo "Time taken: $DURATION seconds"