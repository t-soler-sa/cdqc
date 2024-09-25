# remove_duplicate_v3.sh

#!/bin/bash

# Function to validate the input
validate_input() {
  if [[ "$1" =~ ^[1-9]$|^1[0-2]$|^0[1-9]$ ]]; then
    return 0
  else
    return 1
  fi
}

# Function to pad the month number if needed
pad_month() {
  if [[ "$1" =~ ^[1-9]$ ]]; then
    echo "0$1"
  else
    echo "$1"
  fi
}

# Prompt the user for input until a valid number is provided
while true; do
  read -p "Please input a valid month number (1-12 or 01-12): " month
  if validate_input "$month"; then
    month=$(pad_month "$month")
    break
  else
    echo "Invalid input. Please enter a number between 1 and 12 or 01 and 12."
  fi
done

# Define the year and the base paths
YEAR="2024"
BASE_INPUT_PATH="X:\\INVDESPRO\\INVESTIGACION\\Fondos éticos\\3. DATASETS\\04_Datos Clarity\\01_Equities_feed\\01_Ficheros_originales (Descarga en bruto)"
BASE_OUTPUT_PATH="X:\\INVDESPRO\\INVESTIGACION\\Fondos éticos\\3. DATASETS\\04_Datos Clarity\\01_Equities_feed\\02_Ficheros_tratados\\2024\\output_tristan"

# Construct the input and output file names
INPUT_FILE="${BASE_INPUT_PATH}\\${YEAR}${month}01_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
OUTPUT_FILE="${BASE_OUTPUT_PATH}\\t_${YEAR}${month}01_Equities_feed_IssuerLevel_sinOVR.csv"

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
