import logging
import os
import time

import pandas as pd

# start logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# start timer
start = time.time()

# Ask user input date

DATE = input("Enter date (YYYYMM): ")

# read dataframe
logging.info(f"Reading datafeed for {DATE}")
df = pd.read_csv(
    rf"C:/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/datafeeds_with_ovr/{DATE}01_datafeed_with_ovr.csv",
    low_memory=False,
)

# filter data by region into different dataframes
logging.info("Filtering data by region")
allowed_regions = [
    "N America",
    "Europe",
    "Asia Pacific",
    "Latam",
    "Emerging Markets",
    "no_region",
]

north_america = df[df["region"] == "N America"]
europe = df[df["region"] == "Europe"]
asia_pacific = df[df["region"] == "Asia Pacific"]
latam = df[df["region"] == "Latam"]
emerging_markets = df[df["region"] == "Emerging Markets"]
# dataframe for rows without region
no_region = df[df["region"].isnull()]


# save dataframes
df_list = [north_america, europe, asia_pacific, latam, emerging_markets, no_region]
# make directory if directory does not exist
logging.info("Saving dataframes")
OUTPUT_DIR = rf"C:/Users/n740789/Documents/Projects_local/DataSets/DATAFEED/ficheros_tratados/Feed_region/{DATE}"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

for i in range(len(df_list)):
    # print number rows for each dataframe
    logging.info(f"Number of rows for {allowed_regions[i]}: {df_list[i].shape[0]}")
    df_list[i].to_csv(
        rf"{OUTPUT_DIR}/Equities_{allowed_regions[i]}_{DATE}.csv", index=False
    )

logging.info(f"Dataframes saved to {OUTPUT_DIR}")

end = time.time()
run_time = end - start
# format run time XX min XX sec
minutes = int(run_time // 60)
seconds = int(run_time % 60)
logging.info(f"Script took {minutes} and {seconds} seconds to run")
