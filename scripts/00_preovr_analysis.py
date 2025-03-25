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
    save_excel,
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

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# DEFINE CONTANTSS
# define column's lists and dict

clarity_test_col = [
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_003b_ec",
    "str_004_asec",
    "str_005_ec",
    "art_8_basicos",
    "str_006_sec",
    "str_007_sect",
    "cs_001_sec",
    "cs_003_sec",
    "cs_002_ec",
    "gp_esccp",
    "gp_esccp_22",
    "gp_esccp_25",
    "gp_esccp_30",
]
columns_to_read = ["permid", "isin", "issuer_name"] + clarity_test_col
brs_test_cols = [
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_003b_ec",
    "str_004_asec",
    "str_005_ec",
    "str_006_sec",
    "str_007_sect",
    "str_008_sec",
    "scs_001_sec",
    "scs_002_ec",
    "scs_003_sec",
    "gp_essccp",
    "gp_esccp_22",
    "gp_esccp_25",
    "gp_esccp_30",
    "aladdin_id",
]
rename_dict = {
    "cs_001_sec": "scs_001_sec",
    "cs_002_ec": "scs_002_ec",
    "cs_003_sec": "scs_003_sec",
    "gp_esccp": "gp_essccp",
    "art_8_basicos": "str_008_sec",
}
delta_test_cols = [
    "gp_esccp_22",
    "gp_esccp_25",
    "gp_esccp_30",
    "gp_essccp",
    "scs_001_sec",
    "scs_002_ec",
    "scs_003_sec",
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_003b_ec",
    "str_004_asec",
    "str_005_ec",
    "str_006_sec",
    "str_007_sect",
    "str_008_sec",
]


# DEFINE FUNCTIONS
def prepare_dataframes(
    base_df: pd.DataFrame, new_df: pd.DataFrame, target_index: str = "permid"
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepare DataFrames by setting the index and filtering for common indexes.
    Logs info about common, new, and missing indexes.
    """
    # Set index to 'permid' if it exists, otherwise assume it's already the index.
    logger.info(f"Setting index to {target_index}.")
    if target_index in base_df.columns:
        base_df = base_df.set_index(target_index)
    else:
        logger.warning("df1 does not contain a 'permid' column. Using current index.")

    if target_index in new_df.columns:
        new_df = new_df.set_index(target_index)
    else:
        logger.warning("df2 does not contain a 'permid' column. Using current index.")

    common_indexes = base_df.index.intersection(new_df.index)
    new_indexes = new_df.index.difference(base_df.index)
    missing_indexes = base_df.index.difference(new_df.index)

    logger.info(f"Number of common indexes: {len(common_indexes)}")

    return (
        base_df.loc[common_indexes],
        new_df.loc[common_indexes],
        new_df.loc[new_indexes],
        base_df.loc[missing_indexes],
    )


def compare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, test_col: List[str] = delta_test_cols
) -> pd.DataFrame:
    """Compare DataFrames and create a delta DataFrame."""
    delta = df2.copy()
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Comparing column: {col}")
            # Create a mask for differences between the two DataFrames
            diff_mask = df1[col] != df2[col]
            # Update the delta DataFrame with the differences
            delta.loc[~diff_mask, col] = np.nan
    return delta


def get_exclusion_list(
    row: pd.Series,
    df1: pd.DataFrame,
    test_col: List[str] = delta_test_cols,
) -> List[str]:
    """Get list of columns that changed to EXCLUDED."""
    return [
        col
        for col in test_col
        if row[col] == "EXCLUDED" and df1.loc[row.name, col] != "EXCLUDED"
    ]


def get_inclusion_list(
    row: pd.Series,
    df1: pd.DataFrame,
    test_col: List[str] = delta_test_cols,
) -> List[str]:
    """Get list of columns that changed from EXCLUDED to any other value."""
    return [
        col
        for col in test_col
        if row[col] != "EXCLUDED" and df1.loc[row.name, col] == "EXCLUDED"
    ]


def check_new_exclusions(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    delta: pd.DataFrame,
    test_col: List[str] = delta_test_cols,
    suffix_level: str = "",
) -> pd.DataFrame:
    """Check for new exclusions and update the delta DataFrame."""
    delta["new_exclusion"] = False
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Checking for new exclusions in column: {col}")
            mask = (df1[col] != "EXCLUDED") & (df2[col] == "EXCLUDED")
            delta.loc[mask, "new_exclusion"] = True
            logger.info(f"Number of new exclusions in {col}: {mask.sum()}")
    delta[f"exclusion_list{suffix_level}"] = delta.apply(
        lambda row: get_exclusion_list(row, df1, test_col), axis=1
    )
    return delta


def check_new_inclusions(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    delta: pd.DataFrame,
    test_col: List[str] = delta_test_cols,
    suffix_level: str = "",
) -> pd.DataFrame:
    """Check for new inclusions and update the delta DataFrame."""
    delta["new_inclusion"] = False
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Checking for new inclusions in column: {col}")
            mask = (df1[col] == "EXCLUDED") & (df2[col] != "EXCLUDED")
            delta.loc[mask, "new_inclusion"] = True
            logger.info(f"Number of new inclusions in {col}: {mask.sum()}")
    delta[f"inclusion_list{suffix_level}"] = delta.apply(
        lambda row: get_inclusion_list(row, df1, test_col), axis=1
    )
    return delta


def finalize_delta(
    delta: pd.DataFrame,
    test_col: List[str] = delta_test_cols,
    target_index: str = "permid",
) -> pd.DataFrame:
    """Finalize the delta DataFrame by removing unchanged rows and resetting the index."""
    delta = delta.dropna(subset=test_col, how="all")
    delta.reset_index(inplace=True)
    delta[target_index] = delta[target_index].astype(str)
    logger.info(f"Final delta shape: {delta.shape}")
    return delta


def override_dict(
    df: pd.DataFrame = None,
    id_col: str = "aladdin_id",
    str_col: str = "ovr_target",
    ovr_col: str = "ovr_value",
):
    """
    Converts the overrides DataFrame to a dictionary.
    Args:
        df (pd.DataFrame): DataFrame containing the overrides.
        id_col (str): Column name for the identifier.
        str_col (str): Column name for the strategy.
        ovr_col (str): Column name for the override value.
    Returns:
        dict: Dictionary of overrides.
    """
    # 1. Groupd the df by issuer_id
    grouped = df.groupby(id_col)

    # 2. Initialise the dictionary
    ovr_dict = {}

    # 3. Iterate over each group (issuer id and its corresponding rows)
    for id, group_data in grouped:
        # 3.1. for each issuer id create a dict pairing the strategy and the override value
        ovr_result = dict(zip(group_data[str_col], group_data[ovr_col]))
        # 3.2. add the dict to the main dict
        ovr_dict[id] = ovr_result

    return ovr_dict


def add_portfolio_info_to_df(portfolio_dict, delta_df):
    # Build a reverse lookup dictionary: aladdin_id -> [portfolio_id, strategy_name]
    aladdin_to_info = {}
    for portfolio_id, data in portfolio_dict.items():
        strategy = data.get("strategy_name")
        for a_id in data.get("aladdin_id", []):
            # If the same aladdin_id appears multiple times for a portfolio,
            # this assignment is idempotent.
            aladdin_to_info[a_id] = [portfolio_id, strategy]

    # Add a new column to the DataFrame by mapping the 'aladdin_id' to its portfolio info.
    delta_df["affected_portfolio_str"] = delta_df["aladdin_id"].apply(
        lambda x: aladdin_to_info.get(x)
    )

    return delta_df


# TO DO define function to add override and aladdin data


def main():
    logger.info(f"Starting pre-ovr-analysis for {DATE}.")
    # LOAD DATA
    # clarity data
    df_1 = load_clarity_data(df_1_path, columns_to_read)  # we need overrides
    df_2 = load_clarity_data(df_2_path, columns_to_read)
    logger.info(
        f"previous clarity df's  rows: {df_1.shape[0]}, new clarity df's rows: {df_2.shape[0]}"
    )
    # aladdin /brs data / perimetros
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    crosreference = load_crossreference(CROSSREFERENCE_PATH)
    # ESG Team data: Overrides & Portfolios
    overrides = load_overrides(OVR_PATH)
    override_dict = override_dict(overrides)
    # Load portfolios & benchmarks dicts
    (
        portfolio_dict,
        benchmark_dict,
    ) = load_portfolios(path_pb=BMK_PORTF_STR_PATH, path_committe=COMMITTEE_PATH)

    # PREP DATA FOR ANALYSIS
    # rename column brs_id to aladdin_id
    overrides.rename(columns={"brs_id": "aladdin_id"}, inplace=True)
    ovr_dict = override_dict(overrides)
    # add aladdin_id to df_1 and df_2
    logger.info("Adding aladdin_id to clarity dfs")
    df_1 = df_1.merge(crosreference[["permid", "aladdin_id"]], on="permid", how="left")
    df_2 = df_2.merge(crosreference[["permid", "aladdin_id"]], on="permid", how="left")
    # get BRS data at issuer level
    brs_carteras_issuerlevel = brs_carteras.drop_duplicates(
        subset=["aladdin_id"]
    ).copy()
    # drop row with empty aladdin_id
    brs_carteras_issuerlevel = brs_carteras_issuerlevel[
        brs_carteras_issuerlevel["aladdin_id"].notnull()
    ]

    # PREPARE DATA CLARITY LEVEL
    (
        df_1,
        df_2,
        new_issuers_clarity,
        out_issuer_clarity,
    ) = prepare_dataframes(df_1, df_2)

    # log size of new and missing issuers
    logger.info(
        f"Number of new issuers in Clarity's df: {new_issuers_clarity.shape[0]}"
    )
    logger.info(
        f"Number of missing issuers in Clarity's df: {out_issuer_clarity.shape[0]}"
    )

    # PREPARE DATA BRS LEVEL
    (
        brs_df,
        clarity_df,
        in_clarity_but_not_in_brs,
        in_brs_but_not_in_clarity,
    ) = prepare_dataframes(brs_carteras_issuerlevel, df_2, target_index="aladdin_id")

    # log size of new and missing issuers
    logger.info(
        f"Number issuers in clarity but not Aladdin: {in_clarity_but_not_in_brs.shape[0]}"
    )
    logger.info(
        f"Number issuers in Aladdin but not Clarity: {in_brs_but_not_in_clarity.shape[0]}"
    )

    # START PRE-OVR ANALYSIS
    # COMPARE DATA
    logger.info("comparing clarity dataframes")
    delta_clarity = compare_dataframes(df_1, df_2)
    delta_clarity = check_new_exclusions(df_1, df_2, delta_clarity)
    delta_clarity = check_new_inclusions(df_1, df_2, delta_clarity)
    delta_clarity = finalize_delta(delta_clarity)
    logger.info("checking impact compared to BRS data")
    delta_brs = compare_dataframes(brs_df, clarity_df)
    delta_brs = check_new_exclusions(brs_df, clarity_df, delta_brs, suffix_level="_brs")
    delta_brs = check_new_inclusions(brs_df, clarity_df, delta_brs, suffix_level="_brs")
    delta_brs = finalize_delta(delta_brs, target_index="aladdin_id")

    logger.info("Getting zombie analysis df")
    zombie_df = zombie_killer()

    # PREP DELTAS BEFORE SAVING
    # use crossreference to add permid to delta_brs
    delta_brs = delta_brs.merge(
        crosreference[["aladdin_id", "permid"]], on="aladdin_id", how="left"
    )
    # drop isin from delta_clarity
    delta_clarity.drop(columns=["isin"], inplace=True)
    # add new column to delta_brs with ovr_dict value using aladdin_id
    delta_brs["ovr_list"] = delta_brs["aladdin_id"].map(ovr_dict)
    delta_clarity["ovr_list"] = delta_clarity["aladdin_id"].map(ovr_dict)
    # let's add portfolio info to the delta_df
    delta_clarity = add_portfolio_info_to_df(portfolio_dict, delta_clarity)
    delta_brs = add_portfolio_info_to_df(portfolio_dict, delta_brs)

    # create dict of df and df name
    dfs_dict = {
        "zombie_analysis": zombie_df,
        "summary_brs": delta_brs,
        "summary_clarity": delta_clarity,
    }

    # save to excel
    save_excel(dfs_dict, OUTPUT_DIR, file_name="pre_ovr_analysis")


if __name__ == "__main__":
    main()
