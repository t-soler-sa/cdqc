#!/bin/bash

# Record the start time
start_time=$(date +%s)

# Validate at least one argument
if [ $# -lt 1 ]; then
    echo "Please provide a date parameter (format: yyyymm)"
    echo "Example: ./run_pre_ovr_pipeline.sh 202411 simple"
    exit 1
fi

# Validate date format
if ! [[ $1 =~ ^[0-9]{6}$ ]]; then
    echo "Invalid date format. Please use yyyymm (e.g., 202411)"
    exit 1
fi

# Assign the first parameter to the DATE variable
DATE=$1
SIMPLE_FLAG=""
ZOMBIE_FLAG=""
SCRIPTS=(
    "utils/remove_duplicates.py"
    "utils/update_ovr_db_active_col.py"
    "_00_preovr_analysis.py"
)

# Shift to remove first argument (date), then parse the rest
shift

for arg in "$@"; do
    case "$arg" in
        simple)
            echo "Simple parameter provided! Simplified override analysis will be generated"
            SIMPLE_FLAG="--simple"
            ;;
        only_preovr)
            echo "Only pre override analysis will be generated"
            SCRIPTS=("_00_preovr_analysis.py")
            ;;
        zombie)
            echo "Zombie parameter provided! Zombie analysis will be generated"
            ZOMBIE_FLAG="--zombie"
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Valid options after the date are: 'simple', 'only_preovr'"
            exit 1
            ;;
    esac
done


# Define the base directory
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."

# Activate virtual environment
source "${BASE_DIR}/.venv/Scripts/activate"

# Execute Python scripts in sequence as modules
for script in "${SCRIPTS[@]}"; do
    script_module=${script%.py}
    script_module=${script_module//\//.} # Convert file path to module path
    
    echo "Running $script_module"

    CMD=(python -m "scripts.${script_module}")
    
    # If it's the pre override script, add flags
    if [[ $script_module == "_00_preovr_analysis" ]]; then
        [[ -n $SIMPLE_FLAG ]] && CMD+=("$SIMPLE_FLAG")
        [[ -n $ZOMBIE_FLAG ]] && CMD+=("$ZOMBIE_FLAG")
    fi

    CMD+=(--date "$DATE")
    
    echo "Command: ${CMD[*]}"
    "${CMD[@]}"

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