import glob
import os

import pandas as pd

# Specify the absolute path to the directory containing the CSV files
directory = r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\Feed_region\202408"

# Find all CSV files in the specified directory
csv_files = glob.glob(os.path.join(directory, "*.csv"))

# Loop through all the CSV files and convert them to XLSX for csv_file in csv_files:
for csv_file in csv_files:

    if "Equities_Europe" in csv_file:
        print(f"ignore {csv_f}")
        pass
    else:
        # Read the CSV file with dtype for column 4 as str and low_memory=false
        df = pd.read_csv(csv_file, dtype={4: str}, low_memory=False)

        # Create the XLSX file name by replacing the .csv extension with .xlsx
        xlsx_file = csv_file.replace(".csv", ".xlsx")

        # Save the DataFrame as XLSX in the same directory
        df.to_excel(xlsx_file, index=False)
        print(
            f"Converted {os.path.basename(csv_file)} to {os.path.basename(xlsx_file)}"
        )

print("All CSV files have been converted.")
