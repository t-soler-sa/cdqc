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

# IMPORT MODULS & LIBS
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

from utils.clarity_data_quality_control_functions import (
    prepare_dataframes,
    compare_dataframes,
    check_new_exclusions,
    check_new_inclusions,
    finalize_delta,
    create_override_dict,
    add_portfolio_benchmark_info_to_df,
    get_issuer_level_df,
    filter_non_empty_lists,
    filter_rows_with_common_elements,
    reorder_columns,
    remove_matching_rows,
    clean_inclusion_list,
    clean_portfolio_and_exclusion_list,
    clean_exclusion_list_with_ovr,
)

from utils.zombie_killer import main as zombie_killer

# Import the centralized configuration
from utils.config import get_config

# CONFIG SCRIPT
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
DF_PREV_PATH = paths["PRE_DF_WOVR_PATH"]
DF_NEW_PATH = paths["CURRENT_DF_WOUTOVR_PATH"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]
BMK_PORTF_STR_PATH = paths["BMK_PORTF_STR_PATH"]
OVR_PATH = paths["OVR_PATH"]
COMMITTEE_PATH = paths["COMMITTEE_PATH"]
# Define the output directory and file based on the configuration.
OUTPUT_DIR = config["OUTPUT_DIR"]
OUTPUT_FILE = OUTPUT_DIR / f"{DATE}_pre_ovr_analysis.xlsx"
# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# DEF CONSTANTS
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
rename_dict = {
    "cs_001_sec": "scs_001_sec",
    "cs_002_ec": "scs_002_ec",
    "art_8_basicos": "str_sfdr8_aec",
}


# DEF MAIN
def main():
    logger.info(f"Starting pre-ovr-analysis for {DATE}.")
    # 1.    LOAD DATA

    # 1.1.  aladdin /brs data / perimeters
    logger.info("Loading BRS data")
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    crosreference = load_crossreference(CROSSREFERENCE_PATH)
    # get BRS data at issuer level for becnhmarks without empty aladdin_id
    brs_carteras_issuerlevel = get_issuer_level_df(brs_carteras, "aladdin_id")
    # get BRS data at issuer level for becnhmarks without empty aladdin_id
    brs_benchmarks_issuerlevel = get_issuer_level_df(brs_benchmarks, "aladdin_id")

    # 1.2.  clarity data
    logger.info("Loading clarity data")
    df_1 = load_clarity_data(DF_PREV_PATH, columns_to_read)
    df_2 = load_clarity_data(DF_NEW_PATH, columns_to_read)
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
    logger.info("Loading SRI Team Data: Overrides, Portfolios & Benchmark SRI Strategy")
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
    logger.info("Preparing dataframes for clarity level")
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
    logger.info("Preparing dataframes for BRS Portfolio level")
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
    logger.info("Preparing dataframes for BRS benchmarks level")
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
    logger.info("Starting pre-ovr-analysis")
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
        delta_benchmarks, "affected_benchmark_str"
    )

    # ADD TEST FOR INCLUSIONS
    logger.info("creating copy of dataset for inclusion analysis")
    dlt_inc_brs = delta_brs.copy()
    dlt_inc_benchmarks = delta_benchmarks.copy()

    # pass filter_rows_with_common_elements for columns exclusion_list_brs and affected_portfolio_str
    delta_brs = filter_rows_with_common_elements(
        delta_brs, "exclusion_list_brs", "affected_portfolio_str"
    )
    delta_benchmarks = filter_rows_with_common_elements(
        delta_benchmarks, "exclusion_list_brs", "affected_benchmark_str"
    )

    # let's reset df1 index to permid
    df_1.reset_index(inplace=True)
    df_1["permid"] = df_1["permid"].astype(str)

    # reoder columns for inclusions
    dlt_inc_brs = reorder_columns(dlt_inc_brs, id_name_issuers_cols, delta_test_cols)
    dlt_inc_benchmarks = reorder_columns(
        dlt_inc_benchmarks, id_name_issuers_cols, delta_test_cols
    )

    dlt_inc_brs = clean_inclusion_list(dlt_inc_brs)
    dlt_inc_benchmarks = clean_inclusion_list(dlt_inc_benchmarks)

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
    permid_to_df1 = df_1  # df_1 already has permid as index
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

    # 7. PREP BEFORE SAVING INTO EXCEL

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

    # remove matchin rows from strategies dataframes
    for df_name, df in str_dfs_dict.items():
        str_dfs_dict[df_name] = remove_matching_rows(df)

    # columns to remove from delta_brs, delta_benchmarks before saving
    exclusion_cols_to_remove = ["new_inclusion", "inclusion_list_brs"]
    # columns to remove from dlt_inc_brs and dlt_inc_benchmarks before saving
    inclusion_cols_to_remove = ["new_exclusion", "exclusion_list_brs"]

    # Filter and clean exclusion data
    delta_brs = delta_brs.drop(columns=exclusion_cols_to_remove)
    delta_brs = delta_brs[delta_brs["new_exclusion"] == True]

    delta_benchmarks = delta_benchmarks.drop(columns=exclusion_cols_to_remove)
    delta_benchmarks = delta_benchmarks[delta_benchmarks["new_exclusion"] == True]

    # Filter and clean inclusion data
    dlt_inc_brs = dlt_inc_brs.drop(columns=inclusion_cols_to_remove)
    dlt_inc_brs = dlt_inc_brs[dlt_inc_brs["new_inclusion"] == True]

    dlt_inc_benchmarks = dlt_inc_benchmarks.drop(columns=inclusion_cols_to_remove)
    dlt_inc_benchmarks = dlt_inc_benchmarks[dlt_inc_benchmarks["new_inclusion"] == True]

    # Clean delta_clarity
    delta_clarity.drop(columns=["new_inclusion", "inclusion_list"], inplace=True)
    delta_clarity = delta_clarity[delta_clarity["new_exclusion"] == True]

    # Final cleanup: drop 'new_exclusion' and 'new_inclusion' if present
    for df in [
        delta_brs,
        delta_benchmarks,
        dlt_inc_brs,
        dlt_inc_benchmarks,
        delta_clarity,
    ]:
        for col in ["new_exclusion", "new_inclusion"]:
            if col in df.columns:
                df.drop(columns=col, inplace=True)

    # clean exclusion list if there is overrides ok
    delta_brs = clean_exclusion_list_with_ovr(delta_brs)
    delta_benchmarks = clean_exclusion_list_with_ovr(delta_benchmarks)

    # cleant portfolio and exclusion list
    delta_brs = delta_brs.apply(clean_portfolio_and_exclusion_list, axis=1)
    delta_benchmarks = delta_benchmarks.apply(
        clean_portfolio_and_exclusion_list, axis=1
    )

    # 8 SAVE INTO EXCEL

    # create dict of df and df name
    dfs_dict = {
        "zombie_analysis": zombie_df,
        "excl_carteras": delta_brs,
        "excl_benchmarks": delta_benchmarks,
        "excl_clarity": delta_clarity,
        "incl_carteras": dlt_inc_brs,
        "incl_benchmarks": dlt_inc_benchmarks,
        "new_issuers_clarity": new_issuers_clarity,
        "out_issuer_clarity": out_issuer_clarity,
    }

    # add to dfs_dict the str_dfs_dict
    dfs_dict.update(str_dfs_dict)

    # save to excel
    save_excel(dfs_dict, OUTPUT_DIR, file_name="pre_ovr_analysis")


if __name__ == "__main__":
    main()
