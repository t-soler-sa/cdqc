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
    "utils/remove_duplicates.py"
    "utils/update_ovr_db_active_col.py"
    "_00_preovr_analysis.py"
)

# Activate virtual environment
source "${BASE_DIR}/.venv/Scripts/activate"

# Execute Python scripts in sequence as modules
for script in "${SCRIPTS[@]}"; do
    script_module=${script%.py}
    script_module=${script_module//\//.} # Convert file path to module path
    echo "Running $script_module"
    python -m "scripts.${script_module}" "$DATE"
    
    # Check if the script executed successfully
    if [ $? -ne 0 ]; then
        echo "Error occurred while running $script_module"
        exit 1
    fi
done

echo "All Python scripts completed successfully"
echo "Pre Override Data pipeline Completed successfully. Pre-ovr-analysis ready."

# Calculate and display the total execution time
end_time=$(date +%s)
total_time=$((end_time - start_time))
minutes=$((total_time / 60))
seconds=$((total_time % 60))
echo "Time: $minutes min, $seconds seconds."