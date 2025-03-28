# pre-ovr-analysis
"""
Script to compare the new data from clarity against, previous clarity deliveries,
data on BRS' Aladdom, and our override database.
The script will output an Excel file with the following sheets:
- Zombie Analysis: Summary of data that is in BRS but not in Clarity
- Summary of changes in clarity data using portfolio data in BRS
- Summary of changes in clarity data using benchmark data in BRS
- Summary of changes in clarity data against previous data with overrides
- Summary of changes at the strategy level, one strategy per sheet

"""

import warnings
from typing import List, Tuple
from itertools import chain
from collections import defaultdict

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
OUTPUT_FILE = OUTPUT_DIR / f"{DATE}_pre_ovr_analysis.xlsx"

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# DEFINE CONTANTSS

# let's define necessary column lists & dictionaries
id_name_cols = ["permid", "isin", "issuer_name"]
id_name_issuers_cols = ["aladdin_id", "permid", "issuer_name"]
clarity_test_col = [
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_003b_ec",
    "str_004_asec",
    "str_005_ec",
    "art_8_basicos",
    "str_006_sec",
    "cs_001_sec",
    "cs_002_ec",
]
delta_test_cols = [
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_003b_ec",
    "str_004_asec",
    "str_005_ec",
    "str_006_sec",
    "str_sfdr8_aec",
    "scs_001_sec",
    "scs_002_ec",
]

columns_to_read = id_name_cols + clarity_test_col
brs_test_cols = ["aladdin_id"] + delta_test_cols

rename_dict = {
    "cs_001_sec": "scs_001_sec",
    "cs_002_ec": "scs_002_ec",
    "art_8_basicos": "str_sfdr8_aec",
}


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


def create_override_dict(
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


def add_portfolio_benchmark_info_to_df(
    portfolio_dict, delta_df, column_name="affected_portfolio_str"
):

    # Initialize a defaultdict to accumulate (portfolio_id, strategy_name) pairs
    aladdin_to_info = defaultdict(list)

    for portfolio_id, data in portfolio_dict.items():
        strategy = data.get("strategy_name")
        for a_id in data.get("aladdin_id", []):
            aladdin_to_info[a_id].append((portfolio_id, strategy))

    # Map each aladdin_id in delta_df to a list of accumulated portfolio info
    delta_df[column_name] = delta_df["aladdin_id"].apply(
        lambda x: list(chain.from_iterable(aladdin_to_info.get(x, [])))
    )

    return delta_df


def get_issuer_level_df(df: pd.DataFrame, idx_name: str) -> pd.DataFrame:
    """
    Removes duplicates based on idx_name, and drops rows where idx_name column contains
    NaN, None, or strings like "nan", "NaN", "none", or empty strings.

    Args:
        df (pd.DataFrame): Input dataframe.
        idx_name (str): Column name used for duplicate removal and NaN filtering.

    Returns:
        pd.DataFrame: Cleaned dataframe.
    """
    # Drop duplicates
    df_cleaned = df.drop_duplicates(subset=[idx_name])

    # Drop rows where idx_name is NaN/None or has invalid strings
    valid_rows = df_cleaned[idx_name].notnull() & (
        ~df_cleaned[idx_name]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(["nan", "none", ""])
    )

    return df_cleaned[valid_rows]


def filter_non_empty_lists(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Returns a DataFrame filtered so that rows where the specified column contains
    an empty list are removed. Keeps rows where the column has a list with at least one element.

    Parameters:
    - df (pd.DataFrame): The input DataFrame
    - column (str): The name of the column to check

    Returns:
    - pd.DataFrame: Filtered DataFrame
    """
    return df[df[column].apply(lambda x: isinstance(x, list) and len(x) > 0)]


def filter_rows_with_common_elements(df, col1, col2):
    """
    Return rows of df where the lists in col1 and col2 have at least one common element.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        col1 (str): The name of the first column containing lists.
        col2 (str): The name of the second column containing lists.

    Returns:
        pd.DataFrame: A DataFrame filtered to include only rows where col1 and col2 have a common element.
    """
    logger.info(f"Filtering rows with common elements in columns: {col1} and {col2}")
    mask = df.apply(lambda row: bool(set(row[col1]).intersection(row[col2])), axis=1)
    return df[mask].copy()


def reorder_columns(df: pd.DataFrame, keep_first: list[str], exclude: list[str] = None):
    if exclude is None:
        exclude = set()
    return df[
        keep_first
        + [col for col in df.columns if col not in keep_first and col not in exclude]
    ]


def main():
    logger.info(f"Starting pre-ovr-analysis for {DATE}.")
    # 1.    LOAD DATA

    # 1.1.  aladdin /brs data / perimeters
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    crosreference = load_crossreference(CROSSREFERENCE_PATH)
    # get BRS data at issuer level for becnhmarks without empty aladdin_id
    brs_carteras_issuerlevel = get_issuer_level_df(brs_carteras, "aladdin_id")
    # get BRS data at issuer level for becnhmarks without empty aladdin_id
    brs_benchmarks_issuerlevel = get_issuer_level_df(brs_benchmarks, "aladdin_id")

    # 1.2.  clarity data
    df_1 = load_clarity_data(df_1_path, columns_to_read)
    df_2 = load_clarity_data(df_2_path, columns_to_read)
    # let's rename columns in df_1 and df_2 using the rename_dict
    df_1.rename(columns=rename_dict, inplace=True)
    df_2.rename(columns=rename_dict, inplace=True)
    # add aladdin_id to df_1 and df_2
    logger.info("Adding aladdin_id to clarity dfs")
    df_1 = df_1.merge(crosreference[["permid", "aladdin_id"]], on="permid", how="left")
    df_2 = df_2.merge(crosreference[["permid", "aladdin_id"]], on="permid", how="left")

    logger.info(
        f"previous clarity df's  rows: {df_1.shape[0]}, new clarity df's rows: {df_2.shape[0]}"
    )

    # 1.3.   ESG Team data: Overrides & Portfolios
    overrides = load_overrides(OVR_PATH)
    # rename column brs_id to aladdin_id
    overrides.rename(columns={"brs_id": "aladdin_id"}, inplace=True)
    # rename value column "ovr_target" using rename_dict if value is string
    overrides["ovr_target"] = overrides["ovr_target"].apply(
        lambda x: (
            pd.NA
            if isinstance(x, str) and x.strip().lower() in ["na", "nan"]
            else rename_dict[x] if isinstance(x, str) and x in rename_dict else x
        )
    )
    ovr_dict = create_override_dict(overrides)
    # Load portfolios & benchmarks dicts
    (
        portfolio_dict,
        benchmark_dict,
    ) = load_portfolios(path_pb=BMK_PORTF_STR_PATH, path_committe=COMMITTEE_PATH)

    # 2.    PREP DATA FOR ANALYSIS
    # make sure that the values of of the columns delta_test_cols are strings and all uppercase and strip
    for col in delta_test_cols:
        df_1[col] = df_1[col].str.upper().str.strip()
        df_2[col] = df_2[col].str.upper().str.strip()
        brs_carteras_issuerlevel[col] = (
            brs_carteras_issuerlevel[col].str.upper().str.strip()
        )
        brs_benchmarks_issuerlevel[col] = (
            brs_benchmarks_issuerlevel[col].str.upper().str.strip()
        )

    # 2.2.  PREPARE DATA CLARITY LEVEL
    (
        df_1,
        df_2,
        new_issuers_clarity,
        out_issuer_clarity,
    ) = prepare_dataframes(df_1, df_2)
    # reset index for new_issuers_clarity and out_issuer_clarity
    new_issuers_clarity.reset_index(inplace=True)
    out_issuer_clarity.reset_index(inplace=True)
    # drop isin from out_issuer_clarity and new_issuers_clarity
    out_issuer_clarity.drop(columns=["isin"], inplace=True)
    new_issuers_clarity.drop(columns=["isin"], inplace=True)

    # log size of new and missing issuers
    logger.info(
        f"Number of new issuers in Clarity's df: {new_issuers_clarity.shape[0]}"
    )
    logger.info(
        f"Number of missing issuers in Clarity's df: {out_issuer_clarity.shape[0]}"
    )

    # 2.3.  PREPARE DATA BRS LEVEL FOR PORTFOLIOS
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

    # 2.4.  PREPARE DATA BENCHMARK BRS LEVEL
    (
        brs_df_benchmarks,
        clarity_df_benchmarks,
        in_clarity_but_not_in_brs_benchmarks,
        in_brs_benchmark_but_not_in_clarity,
    ) = prepare_dataframes(brs_benchmarks_issuerlevel, df_2, target_index="aladdin_id")

    # log size of new and missing issuers
    logger.info(
        f"Number issuers in clarity but not benchmarks: {in_clarity_but_not_in_brs_benchmarks.shape[0]}"
    )
    logger.info(
        f"Number issuers in benchmarks but not Clarity: {in_brs_benchmark_but_not_in_clarity.shape[0]}"
    )

    # START PRE-OVR ANALYSIS
    # COMPARE DATA
    logger.info("comparing clarity dataframes")
    delta_clarity = compare_dataframes(df_1, df_2)
    delta_clarity = check_new_exclusions(df_1, df_2, delta_clarity)
    delta_clarity = check_new_inclusions(df_1, df_2, delta_clarity)
    delta_clarity = finalize_delta(delta_clarity)
    logger.info("checking impact compared to BRS portfolio data")
    delta_brs = compare_dataframes(brs_df, clarity_df)
    delta_brs = check_new_exclusions(brs_df, clarity_df, delta_brs, suffix_level="_brs")
    delta_brs = check_new_inclusions(brs_df, clarity_df, delta_brs, suffix_level="_brs")
    delta_brs = finalize_delta(delta_brs, target_index="aladdin_id")
    logger.info("checking impact compared to BRS benchmarks data")
    delta_benchmarks = compare_dataframes(brs_df_benchmarks, clarity_df_benchmarks)
    delta_benchmarks = check_new_exclusions(
        brs_df_benchmarks, clarity_df_benchmarks, delta_benchmarks, suffix_level="_brs"
    )
    delta_benchmarks = check_new_inclusions(
        brs_df_benchmarks, clarity_df_benchmarks, delta_benchmarks, suffix_level="_brs"
    )
    delta_benchmarks = finalize_delta(delta_benchmarks, target_index="aladdin_id")

    # 3.   Get Zombie Analysis
    logger.info("Getting zombie analysis df")
    zombie_df = zombie_killer()

    # 4.    PREP DELTAS BEFORE SAVING
    # PREP DELTAS BEFORE SAVING
    logger.info("Preparing deltas before saving")
    # use crossreference to add permid to delta_brs
    delta_brs = delta_brs.merge(
        crosreference[["aladdin_id", "permid"]], on="aladdin_id", how="left"
    )
    delta_benchmarks = delta_benchmarks.merge(
        crosreference[["aladdin_id", "permid"]], on="aladdin_id", how="left"
    )
    # drop isin from deltas
    delta_clarity.drop(columns=["isin"], inplace=True)
    delta_brs.drop(columns=["isin"], inplace=True)
    delta_benchmarks.drop(columns=["isin"], inplace=True)
    # add new column to delta_brs with ovr_dict value using aladdin_id
    delta_brs["ovr_list"] = delta_brs["aladdin_id"].map(ovr_dict)
    delta_clarity["ovr_list"] = delta_clarity["aladdin_id"].map(ovr_dict)
    delta_benchmarks["ovr_list"] = delta_benchmarks["aladdin_id"].map(ovr_dict)
    # let's add portfolio info to the delta_df
    delta_clarity = add_portfolio_benchmark_info_to_df(portfolio_dict, delta_clarity)
    delta_brs = add_portfolio_benchmark_info_to_df(portfolio_dict, delta_brs)
    delta_benchmarks = add_portfolio_benchmark_info_to_df(
        portfolio_dict, delta_benchmarks
    )
    # let's add benchmark info to the delta_df
    delta_clarity = add_portfolio_benchmark_info_to_df(
        benchmark_dict, delta_clarity, "affected_benchmark_str"
    )
    delta_brs = add_portfolio_benchmark_info_to_df(
        benchmark_dict, delta_brs, "affected_benchmark_str"
    )
    delta_benchmarks = add_portfolio_benchmark_info_to_df(
        benchmark_dict, delta_benchmarks, "affected_benchmark_str"
    )

    # 5. FILTER & SORT DATA & GET RELEVANT DATA FOR THE ANALYSIS
    # let's use filter_non_empty_lists to remove rows with empty lists in affected_portfolio_str
    delta_brs = filter_non_empty_lists(delta_brs, "affected_portfolio_str")
    # let's use filter_non_empty_lists to remove rows with empty lists in affected_portfolio_str
    delta_benchmarks = filter_non_empty_lists(
        delta_benchmarks, "affected_portfolio_str"
    )
    # pass filter_rows_with_common_elements for columns exclusion_list_brs and affected_portfolio_str
    delta_brs = filter_rows_with_common_elements(
        delta_brs, "exclusion_list_brs", "affected_portfolio_str"
    )
    delta_benchmarks = filter_rows_with_common_elements(
        delta_benchmarks, "exclusion_list_brs", "affected_portfolio_str"
    )

    # 6. GET STRATEGIES DFS
    str_dfs_dict = {}

    # Iterate over strategies to build DataFrames
    for strategy in delta_test_cols:
        rows = []

        for _, row in delta_brs.iterrows():
            if strategy in row["exclusion_list_brs"]:
                rows.append(
                    {
                        "aladdin_id": row["aladdin_id"],
                        "permid": row["permid"],
                        "issuer_name": row["issuer_name"],
                        strategy: row[strategy],
                        "affected_portfolio_str": row["affected_portfolio_str"],
                    }
                )

        str_dfs_dict[strategy] = pd.DataFrame(rows)

    # Prepare lookups for efficient mapping
    permid_to_df1 = df_1
    aladdin_to_brs = brs_carteras_issuerlevel.set_index("aladdin_id")

    for strategy_name, df in str_dfs_dict.items():
        # Initialize additional columns
        df[f"{strategy_name}_old"] = None
        df[f"{strategy_name}_brs"] = None
        df[f"{strategy_name}_ovr"] = None

        for i, row in df.iterrows():
            permid = row["permid"]
            aladdin_id = row["aladdin_id"]

            # Lookup from df_1
            if permid in permid_to_df1.index:
                df.at[i, f"{strategy_name}_old"] = permid_to_df1.at[
                    permid, strategy_name
                ]

            # Lookup from brs_carteras_issuerlevel
            if aladdin_id in aladdin_to_brs.index:
                df.at[i, f"{strategy_name}_brs"] = aladdin_to_brs.at[
                    aladdin_id, strategy_name
                ]

            # Lookup from overrides
            match = overrides.loc[
                (overrides["permid"] == permid)
                & (overrides["ovr_target"] == strategy_name),
                "ovr_value",
            ]
            if not match.empty:
                df.at[i, f"{strategy_name}_ovr"] = match.values[0]

        # Move "affected_portfolio_str" to the end
        cols = [col for col in df.columns if col != "affected_portfolio_str"] + [
            "affected_portfolio_str"
        ]
        df = df[cols]
        str_dfs_dict[strategy_name] = df

    # 7. SAVE TO EXCEL

    # 7.1. sort columns of deltas df before saving
    # set id_name_issuers_cols first and exclude delta_test_cols
    delta_brs = reorder_columns(delta_brs, id_name_issuers_cols, delta_test_cols)
    delta_clarity = reorder_columns(
        delta_clarity, id_name_issuers_cols, delta_test_cols
    )
    delta_benchmarks = reorder_columns(
        delta_benchmarks, id_name_issuers_cols, delta_test_cols
    )
    # set id_name_issuers_cols first
    new_issuers_clarity = reorder_columns(
        new_issuers_clarity, id_name_issuers_cols, id_name_cols
    )
    out_issuer_clarity = reorder_columns(
        out_issuer_clarity, id_name_issuers_cols, id_name_cols
    )

    # create dict of df and df name
    dfs_dict = {
        "zombie_analysis": zombie_df,
        "delta_carteras": delta_brs,
        "delta_benchmarks": delta_benchmarks,
        "delta_clarity": delta_clarity,
        "new_issuers_clarity": new_issuers_clarity,
        "out_issuer_clarity": out_issuer_clarity,
    }

    # add to dfs_dict the str_dfs_dict
    dfs_dict.update(str_dfs_dict)

    # save to excel
    save_excel(dfs_dict, OUTPUT_DIR, file_name="pre_ovr_analysis")


if __name__ == "__main__":
    main()
