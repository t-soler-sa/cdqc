import logging
import time
from pathlib import Path

import pandas as pd


# Set up logging info
def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


setup_logging()
# Add timer to the function
start_time = time.time()
logging.info("Script started")

# Define Date
DATE = input("Insert date in the format YYYYMM, please: ")

# Define BASE_DIRECTORY
BASE_DIRECTORY = Path(
    r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ovr"
)

# Define INPUT_PATH
INPUT_PATH = BASE_DIRECTORY / f"{DATE}01_datafeed_with_ovr.csv"

# Define OUTPUT_PATH for Excel file
OUTPUT_PATH = BASE_DIRECTORY / f"{DATE}_df_issuer_level_with_OVR.xlsx"

# Define OUTPUT_PATH for CSV file
OUTPUT_PATH_CSV = BASE_DIRECTORY / f"{DATE}_df_issuer_level_with_ovr.csv"

logging.info("Loading raw dataset")
# Read csv INPUT_PATH
df = pd.read_csv(INPUT_PATH, low_memory=False)

# Lower column names
df.columns = df.columns.str.lower()

logging.info("Removing duplicates by permId")
# Remove duplicate by subset "permid"
df_2 = df.drop_duplicates(subset=["permid"])

# Save to OUTPUT_PATH as csv file
logging.info("Saving dataset at issuer level on a csv file")
df_2.to_csv(OUTPUT_PATH_CSV, index=False)

# Save to OUTPUT_PATH as excel file
# Uncomment the following lines if you want to save as Excel file
logging.info("Saving dataset at issuer level on an Excel file")
df_2.to_excel(OUTPUT_PATH, index=False)

end_time = time.time()
logging.info(f"Script completed in {end_time - start_time:.2f} seconds")
logging.info(f"Files saved in {BASE_DIRECTORY}")
# Display rows before and after
logging.info(f"\nRows before: {df.shape[0]} \nRows after: {df_2.shape[0]}")
