import os
import pandas as pd

# Load the CSV file
file_path = r"C:\Users\n740789\Downloads\20250224_overrides_backfiller_file_brsid.csv"
df = pd.read_csv(file_path)

# Define the output directory
output_dir = r"C:\Users\n740789\Downloads\override_csv_files"
os.makedirs(output_dir, exist_ok=True)

# Override ground columns mapping
override_grounds = {
    "og_srating": "srating",
    "og_exposure": "exposure",
    "og_controversy": "controversy",
    "og_committee": "committee",
}

# Iterate over unique combinations of `ovr_target` and `ovr_value`
for (ovr_target, ovr_value), group in df.groupby(["ovr_target", "ovr_value"]):
    # Iterate over override grounds
    for col, ground in override_grounds.items():
        # Filter rows where the override ground is marked with 'x'
        filtered_group = group[group[col] == "x"]

        if not filtered_group.empty:
            # Define the filename based on the override ground
            file_name = f"ovr_{ovr_target}_{ovr_value}_{ground}.csv"
            file_path = os.path.join(output_dir, file_name)

            # Select only required columns
            filtered_group[["issuer_name", "brs_id"]].to_csv(file_path, index=False)

print(f"CSV files have been saved in {output_dir}")
