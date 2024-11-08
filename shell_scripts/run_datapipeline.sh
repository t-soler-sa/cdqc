#!/bin/bash

# Check if date parameter is provided
if [ $# -eq 0 ]; then
    echo "Please provide a date parameter (format: yyyymm)"
    echo "Example: shell_scripts/run_datapipeline.sh 202411"
    exit 1
fi

DATE=$1

# Array of scripts to run
SCRIPTS=(
    "/c/Users/n740789/Documents/clarity_data_quality_controls/01_overrides_clarityid.py"       
    "/c/Users/n740789/Documents/clarity_data_quality_controls/01_overrides_permid.py"
    "/c/Users/n740789/Documents/clarity_data_quality_controls/02_apply_ow.py"
    "/c/Users/n740789/Documents/clarity_data_quality_controls/03_remove_duplicates_with_ovr.py"
    "/c/Users/n740789/Documents/clarity_data_quality_controls/04_noncomplience.py"
)

# Execute Python scripts in sequence, passing the date parameter
for script in "${SCRIPTS[@]}"; do
    echo "Running $script"
    python "$script" "$DATE"
    
    # Check if the script executed successfully
    if [ $? -ne 0 ]; then
        echo "Error occurred while running $script"
        exit 1
    fi
done

echo "All Python scripts completed successfully"

# Run the split_datafeed_by_region.sh script
SPLIT_SCRIPT="/c/Users/n740789/Documents/clarity_data_quality_controls/shell_scripts/split_datafeed_by_region.sh"

echo "Running $SPLIT_SCRIPT"
# Pass the DATE as an environment variable to the script
export USER_DATE="$DATE"
bash "$SPLIT_SCRIPT"

# Check if the split script executed successfully
if [ $? -ne 0 ]; then
    echo "Error occurred while running $SPLIT_SCRIPT"
    exit 1
fi

echo "Data pipeline completed successfully"