# split_df_by region

import os
from pathlib import Path

import pandas as pd

from scripts.utils.config import get_config

# Get configuration settings
config = get_config(script_name="split_region_datafeed", gen_output_dir=False)
logger = config["logger"]

# Define Date
DATE = config["DATE"]
YEAR = config["YEAR"]

# define BASE_DIRECTORY
# Define paths from configuration
BASE_DIR = config["DATAFEED_DIR"]
OUTPUT_DIR = BASE_DIR / "datafeeds_without_ovr" / "Feed_region" / f"{DATE}"


def main(
    df_path: Path,
    target_region: list[str] = ["Latam"],
):
    # read dataframe
    logger.info(f"Reading datafeed for {DATE}")
    df = pd.read_csv(
        df_path,
        low_memory=False,
    )

    allowed_regions = [
        "N America",
        "Europe",
        "Asia Pacific",
        "Latam",
        "Emerging Markets",
    ]

    if target_region is None:
        target_region = allowed_regions

    # filter data by region into different dataframes
    logger.info("Filtering data by region")

    dfs_regions = []
    for region in target_region:
        reg_df = df[df["region"] == region]
        dfs_regions.append(reg_df)

    # add dataframe for rows without region
    no_region = df[df["region"].isnull()]
    dfs_regions.append(no_region)
    target_region.append("no_region")

    # make directory if directory does not exist
    logger.info("Saving dataframes")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for region, df in zip(target_region, dfs_regions):
        OUTPUT_FILE = OUTPUT_DIR / f"Equities_{region}_{DATE}.csv"
        # print number rows for each dataframe
        logger.info(f"Saving df for region {region}")
        df.to_csv(OUTPUT_FILE, index=False)

    logger.info(f"Dataframes saved to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
