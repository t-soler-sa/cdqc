import logging
import time
from pathlib import Path

import pandas


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
DATE = input("Insert data in the formar YYYYMM, please: ")
# Get year from DATE
YEAR = DATE[:4]

# define BASE_DIRECTORY
BASE_DIRECTORY = Path(r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED")

# Define INPUT_PATH
INPUT_PATH = (
    BASE_DIRECTORY
    / "raw_dataset"
    / f"{YEAR}"
    / f"{DATE}01_Production"
    / f"{DATE}01_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
)

# Define OUTPUT_PATH
OUTPUT_PATH = (
    BASE_DIRECTORY
    / "ficheros_tratados"
    / f"{YEAR}"
    / f"{DATE}01_Equities_feed_IssuerLevel_sinOVR.xlsx"
)  # naming 20240901_Equities_feed_IssuerLevel_sinOVR

logging.info("Loading raw dataset")
# read csv INPUT_PATH
df = pandas.read_csv(INPUT_PATH, low_memory=False)

# Lower column names
df.columns = df.columns.str.lower()

logging.info("Removing duplicates by permId")
# remove duplicate by subset "issuer_name"
df_2 = df.drop_duplicates(subset=["permid"])

# Save to OUTPUT_PATH as csv file
logging.info("Saving dataset at issuer level on a csv file")
csv_output_path = OUTPUT_PATH.with_suffix(".csv")
df_2.to_csv(csv_output_path, index=False)

# logging.info("Saving dataset at issuer level on an Excel file")
## Save to OUTPUT_PATH as excel file
# df_2.to_excel(OUTPUT_PATH, index=False)

end_time = time.time()
logging.info(f"Script completed in {end_time - start_time:.2f} seconds")
# display rows before and after
logging.info(f"FINAL OUTPUT:\nRows before: {df.shape[0]} \nRows after: {df_2.shape[0]}")
