# remove_duplicates_wo_ovr.py

import time

from config import get_config
from dataloaders import load_csv


# Get configuration settings
config = get_config(script_name="remove_duplicates_without_ovr", gen_output_dir=False)
logger = config["logger"]

# Add timer to the function
start_time = time.time()
logger.info("Script started")

# Define Date
DATE = config["DATE"]
YEAR = config["YEAR"]

# define BASE_DIRECTORY
# Define paths from configuration
INPUT_PATH = config["paths"]["RAW_DF_WOUT_OVR_PATH"]
OUTPUT_DIR = config["paths"]["PROCESSED_DFS_WOUTOVR_PATH"]
OUTPUT_PATH = OUTPUT_DIR / f"{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"

logger.info("Loading raw dataset")
# read csv INPUT_PATH
df = load_csv(INPUT_PATH)

logger.info("Removing duplicates by permId")
# remove duplicate by subset "issuer_name"
df_2 = df.drop_duplicates(subset=["permid"]).copy()

# Save to OUTPUT_PATH as csv file
logger.info("Saving dataset at issuer level on a csv file")
df_2.to_csv(OUTPUT_PATH, index=False)
logger.info(f"Saved to {OUTPUT_PATH}")

end_time = time.time()
logger.info(f"Script completed in {end_time - start_time:.2f} seconds")
# display rows before and after
logger.info(f"FINAL OUTPUT:\nRows before: {df.shape[0]} \nRows after: {df_2.shape[0]}")
