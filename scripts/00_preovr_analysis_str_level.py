import sys
import os
import warnings
from pathlib import Path
from typing import List, Tuple
from itertools import chain

import numpy as np
import pandas as pd

from utils.dataloaders import (
    load_clarity_data,
    load_aladdin_data,
    load_crossreference,
    load_portfolios,
    load_overrides,
)
from utils.zombie_killer import main as zombie_killer

# Import the centralized configuration
from config import get_config

# Get the common configuration for the Pre-OVR-Analysis script.
config = get_config("pre-ovr-analysis", interactive=False)
logger = config["logger"]
DATE = config["DATE"]
YEAR = config["YEAR"]
DATE_PREV = config["DATE_PREV"]
REPO_DIR = config["REPO_DIR"]
DATAFEED_DIR = config["DATAFEED_DIR"]
SRI_DATA_DIR = config["SRI_DATA_DIR"]
paths = config["paths"]

# Use the paths from config
df_1_path = paths["PRE_DF_WOVR_PATH"]
df_2_path = paths["CURRENT_DF_WOUTOVR_PATH"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]
BMK_PORTF_STR_PATH = paths["BMK_PORTF_STR_PATH"]
OVR_PATH = paths["OVR_PATH"]
COMMITTEE_PATH = paths["COMMITTEE_PATH"]

# Define the output directory and file based on the configuration.
OUTPUT_DIR = config["OUTPUT_DIR"]
OUTPUT_FILE = OUTPUT_DIR / f"{DATE}_pre_ovr_analysis.xlsx"

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Script-specific constants
# let's define necessary column lists
test_col = [
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_004_asec",
    "str_005_ec",
    "cs_001_sec",
    "gp_esccp",
    "cs_003_sec",
    "cs_002_ec",
    "str_006_sec",
    "str_007_sect",
    "gp_esccp_22",
    "gp_esccp_25",
    "gp_esccp_30",
    "art_8_basicos",
    "str_003b_ec",
]
columns_to_read = ["permid", "isin", "issuer_name"] + test_col


# DEFINE FUNCTIONS
def prepare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepare DataFrames by setting the index and filtering for common indexes.
    Logs info about common, new, and missing indexes.
    """
    # Set index to 'permid' if it exists, otherwise assume it's already the index.
    if "permid" in df1.columns:
        df1 = df1.set_index("permid")
    else:
        logger.warning("df1 does not contain a 'permid' column. Using current index.")

    if "permid" in df2.columns:
        df2 = df2.set_index("permid")
    else:
        logger.warning("df2 does not contain a 'permid' column. Using current index.")

    common_indexes = df1.index.intersection(df2.index)
    new_indexes = df2.index.difference(df1.index)
    missing_indexes = df1.index.difference(df2.index)

    logger.info(f"Number of common indexes: {len(common_indexes)}")

    return (
        df1.loc[common_indexes],
        df2.loc[common_indexes],
        df2.loc[new_indexes],
        df1.loc[missing_indexes],
    )


def compare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, test_col: List[str]
) -> pd.DataFrame:
    """Compare DataFrames and create a delta DataFrame."""
    delta = df2.copy()
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Comparing column: {col}")
            diff_mask = df1[col] != df2[col]
            delta.loc[~diff_mask, col] = np.nan
    return delta


def get_exclusion_list(
    row: pd.Series, df1: pd.DataFrame, test_col: List[str]
) -> List[str]:
    """Get list of columns that changed to EXCLUDED."""
    return [
        col
        for col in test_col
        if row[col] == "EXCLUDED" and df1.loc[row.name, col] != "EXCLUDED"
    ]


def get_inclusion_list(
    row: pd.Series, df1: pd.DataFrame, test_col: List[str]
) -> List[str]:
    """Get list of columns that changed from EXCLUDED to any other value."""
    return [
        col
        for col in test_col
        if row[col] != "EXCLUDED" and df1.loc[row.name, col] == "EXCLUDED"
    ]


def check_new_exclusions(
    df1: pd.DataFrame, df2: pd.DataFrame, delta: pd.DataFrame, test_col: List[str]
) -> pd.DataFrame:
    """Check for new exclusions and update the delta DataFrame."""
    delta["new_exclusion"] = False
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Checking for new exclusions in column: {col}")
            mask = (df1[col] != "EXCLUDED") & (df2[col] == "EXCLUDED")
            delta.loc[mask, "new_exclusion"] = True
            logger.info(f"Number of new exclusions in {col}: {mask.sum()}")
    delta["exclusion_list"] = delta.apply(
        lambda row: get_exclusion_list(row, df1, test_col), axis=1
    )
    return delta


def check_new_inclusions(
    df1: pd.DataFrame, df2: pd.DataFrame, delta: pd.DataFrame, test_col: List[str]
) -> pd.DataFrame:
    """Check for new inclusions and update the delta DataFrame."""
    delta["new_inclusion"] = False
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Checking for new inclusions in column: {col}")
            mask = (df1[col] == "EXCLUDED") & (df2[col] != "EXCLUDED")
            delta.loc[mask, "new_inclusion"] = True
            logger.info(f"Number of new inclusions in {col}: {mask.sum()}")
    delta["inclusion_list"] = delta.apply(
        lambda row: get_inclusion_list(row, df1, test_col), axis=1
    )
    return delta


def finalize_delta(delta: pd.DataFrame, test_col: List[str]) -> pd.DataFrame:
    """Finalize the delta DataFrame by removing unchanged rows and resetting the index."""
    delta = delta.dropna(subset=test_col, how="all")
    delta.reset_index(inplace=True)
    logger.info(f"Final delta shape: {delta.shape}")
    return delta


# TO DO define function to add override and aladdin data


def main():
    # LOAD DATA
    # clarity data
    df_1 = load_clarity_data(df_1_path, columns_to_read)  # we need overrides
    df_2 = load_clarity_data(df_2_path, columns_to_read)
    # aladdin /brs data / perimetros
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    crosreference = load_crossreference(CROSSREFERENCE_PATH)
    # sri/ESG Team data
    overrides = load_overrides(OVR_PATH)

    # Load portfolios and benchmarks data (using a placeholder file path).
    (
        portfolios_dict,
        benchmarks_dict,
        carteras_list,
        benchmarks_list,
        carteras_benchmarks_list,
    ) = load_portfolios("your_file_path.xlsx")

    logger.info(f"df_1 shape: {df_1.shape}, df_2 shape: {df_2.shape}")

    df_1, df_2, new_issuer, out_issuer = prepare_dataframes(df_1, df_2)
    # Log the size of new and missing issuers
    logger.info(f"Number of new issuers: {new_issuer.shape[0]}")
    logger.info(f"Number of missing issuers: {out_issuer.shape[0]}")

    delta = compare_dataframes(df_1, df_2, test_col)
    delta = check_new_exclusions(df_1, df_2, delta, test_col)
    delta = check_new_inclusions(df_1, df_2, delta, test_col)
    delta = finalize_delta(delta, test_col)

    logger.info("Getting zombie analysis df")
    zombie_df = zombie_killer()

    # Create the output directory if it does not exist.
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")
    else:
        print(f"Directory already exists: {OUTPUT_DIR}")

    # Save zombie_df and delta to an Excel file with two sheets.
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        delta.to_excel(writer, sheet_name="pre_ovr_analysis", index=False)
        zombie_df.to_excel(writer, sheet_name="zombie_analysis", index=False)

    logger.info(f"Results saved to {OUTPUT_FILE}")
    logger.info("Analysis completed successfully.")


if __name__ == "__main__":
    main()
