#!/bin/bash

# Record the start time
start_time=$(date +%s)

# Check if date parameter is provided
if [ $# -eq 0 ]; then
    echo "Please provide a date parameter (format: yyyymm)"
    echo "Example: ./run_datapipeline.sh 202411"
    exit 1
fi

# Validate date format
if ! [[ $1 =~ ^[0-9]{6}$ ]]; then
    echo "Invalid date format. Please use yyyymm (e.g., 202411)"
    exit 1
fi

# Assign the first parameter to the DATE variable
DATE=$1

# Define the base directory
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."

# Array of scripts to run
SCRIPTS=(
    "01_overrides_clarityid.py"
    "01_overrides_permid.py"
    "02_apply_ow.py"
    "03_remove_duplicates_with_ovr.py"
    "04_noncomplience.py"
    "05_impact_analysis.py"
)

# Activate virtual environment
source "${BASE_DIR}/venv/bin/activate"

# Execute Python scripts in sequence, passing the date parameter
for script in "${SCRIPTS[@]}"; do
    echo "Running $script"
    python "${BASE_DIR}/${script}" "$DATE"
    
    # Check if the script executed successfully
    if [ $? -ne 0 ]; then
        echo "Error occurred while running $script"
        deactivate  # Deactivate virtual environment before exiting
        exit 1
    fi
done

echo "All Python scripts completed successfully"

# Run the split_datafeed_by_region.sh script
SPLIT_SCRIPT="${BASE_DIR}/shell_scripts/split_datafeed_by_region.sh"

echo "Running $SPLIT_SCRIPT"
# Pass the DATE as an environment variable to the script
export USER_DATE="$DATE"
bash "$SPLIT_SCRIPT"

# Check if the split script executed successfully
if [ $? -ne 0 ]; then
    echo "Error occurred while running $SPLIT_SCRIPT"
    unset USER_DATE  # Clean up environment variable
    deactivate  # Deactivate virtual environment before exiting
    exit 1
fi

echo "Data pipeline completed successfully"

# Calculate and display the total execution time
end_time=$(date +%s)
total_time=$((end_time - start_time))
minutes=$((total_time / 60))
seconds=$((total_time % 60))
echo "Time: $minutes min, $seconds seconds."

# Clean up
unset USER_DATE
deactivate  # Deactivate virtual environment



