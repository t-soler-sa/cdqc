import glob
import logging
import os
import json
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()

# Directory where your CSV files are stored
directory = r"C:\Users\n740789\Documents\Projects_local\DataSets\crossreference"
file_pattern = "Aladdin_Clarity_Issuers_*.csv"
files = sorted(glob.glob(f"{directory}/{file_pattern}"))

# Log the files found
logger.info(f"Found {len(files)} files matching the pattern:")
for file in files:
    logger.info(f"  - {file}")

if len(files) < 2:
    logger.error("Not enough files found to compare. Exiting.")
    exit(1)

# To store the results
summary_data = []

for i in range(1, len(files)):
    prev_file = files[i - 1]
    curr_file = files[i]

    prev_month = os.path.splitext(os.path.basename(prev_file))[0].split("_")[-1]
    curr_month = os.path.splitext(os.path.basename(curr_file))[0].split("_")[-1]

    logger.info(f"\nComparing {prev_month} with {curr_month}")

    try:
        prev_df = pd.read_csv(prev_file)
        curr_df = pd.read_csv(curr_file)
    except Exception as e:
        logger.error(f"Error reading CSV files: {e}")
        continue

    # Check if 'Aladdin_Issuer' column exists
    if (
        "Aladdin_Issuer" not in prev_df.columns
        or "Aladdin_Issuer" not in curr_df.columns
    ):
        logger.error(
            "'Aladdin_Issuer' column not found in one or both files. Skipping comparison."
        )
        continue

    # Get sets of Aladdin_Issuer for each month
    prev_issuers = set(prev_df["Aladdin_Issuer"])
    curr_issuers = set(curr_df["Aladdin_Issuer"])

    # Calculate the changes
    total_prev = len(prev_issuers)
    total_curr = len(curr_issuers)
    new_entries = len(curr_issuers - prev_issuers)
    exited_entries = len(prev_issuers - curr_issuers)

    # Calculate percentages
    percent_in = (new_entries / total_prev) * 100
    percent_out = (exited_entries / total_prev) * 100

    summary_data.append(
        {
            "prev_month": prev_month,
            "curr_month": curr_month,
            "total_prev": total_prev,
            "total_curr": total_curr,
            "new_entries": new_entries,
            "exited_entries": exited_entries,
            "percent_in": round(percent_in, 2),
            "percent_out": round(percent_out, 2),
        }
    )

    logger.info(f"Total unique issuers in previous month: {total_prev}")
    logger.info(f"Total unique issuers in current month: {total_curr}")
    logger.info(f"New unique issuers: {new_entries} ({percent_in:.2f}%)")
    logger.info(f"Exited unique issuers: {exited_entries} ({percent_out:.2f}%)")

# Save summary data to a JSON file
with open("summary_data.json", "w") as f:
    json.dump(summary_data, f)

print("\nSummary data has been saved to 'summary_data.json'")
