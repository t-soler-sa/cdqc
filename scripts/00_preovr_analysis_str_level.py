import argparse
import logging
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from itertools import chain

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from utils.dataloaders import (
    load_clarity_data,
    load_aladdin_data,
    load_crossreference,
    load_portfolios,
    load_overrides,
)

from utils.get_date import get_date
from utils.set_up_log import set_up_log

# Set up logging
logger = set_up_log("Pre-OVR-Analysis")


# Use get_date to obtain a date string
date_str = get_date()

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def prepare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare DataFrames by setting index and filtering for common indexes.
    What do we do with the missing indexes? save them on a csv?
    """

    df1.set_index("permid", inplace=True)
    df2.set_index("permid", inplace=True)
    common_indexes = df1.index.intersection(df2.index)
    logging.info(f"Number of common indexes: {len(common_indexes)}")
    return df1.loc[common_indexes], df2.loc[common_indexes]


def compare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, test_col: List[str]
) -> pd.DataFrame:
    """Compare DataFrames and create delta DataFrame."""
    delta = df2.copy()
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logging.info(f"Comparing column: {col}")
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
    """Check for new exclusions and update delta DataFrame."""
    delta["new_exclusion"] = False
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logging.info(f"Checking for new exclusions in column: {col}")
            mask = (df1[col] != "EXCLUDED") & (df2[col] == "EXCLUDED")
            delta.loc[mask, "new_exclusion"] = True
            logging.info(f"Number of new exclusions in {col}: {mask.sum()}")
    delta["exclusion_list"] = delta.apply(
        lambda row: get_exclusion_list(row, df1, test_col), axis=1
    )
    return delta


def check_new_inclusions(
    df1: pd.DataFrame, df2: pd.DataFrame, delta: pd.DataFrame, test_col: List[str]
) -> pd.DataFrame:
    """Check for new inclusions and update delta DataFrame."""
    delta["new_inclusion"] = False
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logging.info(f"Checking for new inclusions in column: {col}")
            mask = (df1[col] == "EXCLUDED") & (df2[col] != "EXCLUDED")
            delta.loc[mask, "new_inclusion"] = True
            logging.info(f"Number of new inclusions in {col}: {mask.sum()}")
    delta["inclusion_list"] = delta.apply(
        lambda row: get_inclusion_list(row, df1, test_col), axis=1
    )
    return delta


def finalize_delta(delta: pd.DataFrame, test_col: List[str]) -> pd.DataFrame:
    """Finalize delta DataFrame by removing unchanged rows and resetting index."""
    delta = delta.dropna(subset=test_col, how="all")
    delta.reset_index(inplace=True)
    logging.info(f"Final delta shape: {delta.shape}")
    return delta


def main():
    # Get user input for date
    DATE = get_date()
    YEAR = DATE[:4]
    date_obj = datetime.strptime(DATE, "%Y%m")
    prev_date_obj = date_obj - relativedelta(months=1)
    DATE_PREV = prev_date_obj.strftime("%Y%m")

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

    # DEFINE PATHS
    REPO_DIR = Path(r"C:\Users\n740789\Documents\clarity_data_quality_controls")
    DATAFEED_DIR = Path(r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED")
    df_1_path = (
        DATAFEED_DIR
        / "datafeeds_with_ovr"
        / f"{DATE_PREV}_df_issuer_level_with_ovr.csv"
    )
    df_2_path = (
        DATAFEED_DIR
        / "ficheros_tratados"
        / f"{YEAR}"
        / f"{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"
    )
    ALADDIN_DATA_DIR = REPO_DIR / "excel_books" / "aladdin_data"
    CROSSREFERENCE_PATH = (
        ALADDIN_DATA_DIR
        / "crossreference"
        / f"Aladdin_Clarity_CrossReference_{DATE}01.csv"
    )
    BMK_PORTF_STR_PATH = (
        ALADDIN_DATA_DIR
        / "bmk_portf_str"
        / f"{DATE}_strategies_snt world_portf_bmks.xlsx"
    )
    SRI_DATA_DIR = REPO_DIR / "excel_books" / "sri_data"
    OVR_PATH = (
        REPO_DIR
        / "excel_books"
        / "sri_data"
        / "overrides"
        / "20250314_override_analysis_subs.xlsx"
    )
    COMMITTEE_PATH = (
        REPO_DIR
        / "excel_books"
        / "sri_data"
        / "portfolios_committees"
        / "portfolio_lists.xlsx"
    )

    # LOAD DATA
    columns_to_read = ["permid", "isin", "issuer_name"] + test_col
    df_1 = load_clarity_data(df_1_path, columns_to_read)
    df_2 = load_clarity_data(df_2_path, columns_to_read)
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    crosreference = load_crossreference(CROSSREFERENCE_PATH)
    overrides = load_overrides(OVR_PATH)

    # Load portfolios & benchmarks dicts and lists
    (
        portfolios_dict,
        benchmarks_dict,
        carteras_list,
        benchmarks_list,
        carteras_benchmarks_list,
    ) = load_portfolios("your_file_path.xlsx")

    logging.info(f"df_1 shape: {df_1.shape}, df_2 shape: {df_2.shape}")

    df_1, df_2 = prepare_dataframes(df_1, df_2)

    delta = compare_dataframes(df_1, df_2, test_col)

    delta = check_new_exclusions(df_1, df_2, delta, test_col)
    delta = check_new_inclusions(df_1, df_2, delta, test_col)

    delta = finalize_delta(delta, test_col)

    output_file = "delta_results_beta.csv"
    delta.to_csv(output_file, index=False)
    logging.info(f"Results saved to {output_file}")

    logging.info("Analysis completed successfully.")


if __name__ == "__main__":
    main()
