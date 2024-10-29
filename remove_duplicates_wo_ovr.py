import logging
import time

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

# define BASE_DIRECTORY   C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED
BASE_DIRECTORY = r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED"
# define INPUT_PATH = BASE_DIRECTORY \ r'20240701_datafeed_with_ow.csv'
INPUT_PATH = (
    BASE_DIRECTORY
    + rf"\raw_dataset\{DATE}01_Production\{DATE}01_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
)
# define OUTPU_PATH DIRECTORY:
OUTPUT_PATH = (
    BASE_DIRECTORY
    + rf"\ficheros_tratados\{DATE}01_Equities_feed_IssuerLevel_sinOVR.xlsx"
)  # naming 20240901_Equities_feed_IssuerLevel_sinOVR

logging.info("Loading raw dataset")
# read csv INPUT_PATH
df = pandas.read_csv(INPUT_PATH, low_memory=False)

# Lower column names
df.columns = df.columns.str.lower()

logging.info("Removing duplicates by permId")
# remove duplicate by subset "issuer_name"
df_2 = df.drop_duplicates(subset=["permid"])

# save to OUTPUT_PATH as csv file
logging.info("Saving dataset at issuer level on a csv file")
df_2.to_csv(OUTPUT_PATH.replace(".xlsx", ".csv"), index=False)

logging.info("Saving dataset at issuer level on an Excel file")
# save to OUTPUT_PATH as excel file
df_2.to_excel(OUTPUT_PATH, index=False)

end_time = time.time()
logging.info(f"Script completed in {end_time - start_time:.2f} seconds")
# display rows before and after
logging.info(f"\nRows before: {df.shape[0]} \nRows after: {df_2.shape[0]}")
