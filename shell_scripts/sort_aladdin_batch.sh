#!/bin/bash
 
# Define the full path for source and destination directories
SOURCE_DIR="/c/Users/n740789/Documents/Projects_local/DataSets/impact_analysis/new_batch"
DEST_DIR="/c/Users/n740789/Documents/Projects_local/DataSets/impact_analysis/2411"
IMPACT_ANALYSIS_DIR="/c/Users/n740789/Documents/Projects_local/DataSets/impact_analysis"
 
# Create the new directory structure in 2411
mkdir -p "${DEST_DIR}/art_8_basico"
mkdir -p "${DEST_DIR}/ESG"
mkdir -p "${DEST_DIR}/Sustainable"
 
# Move files based on their names
for file in "${SOURCE_DIR}"/*; do
    filename=$(basename "$file")
    
    if [[ $filename =~ Art8Basico ]]; then
        mv "$file" "${DEST_DIR}/art_8_basico/"
    elif [[ $filename =~ ESGFunds ]]; then
        mv "$file" "${DEST_DIR}/ESG/"
    elif [[ $filename =~ SustFunds ]]; then
        mv "$file" "${DEST_DIR}/Sustainable/"
    fi
done
 
echo "Files have been moved to the 2411 directory with the specified structure."