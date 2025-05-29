# pre-ovr-analysis.py
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
import sys
import argparse

import pandas as pd

from scripts.utils.dataloaders import (
    load_clarity_data,
    load_aladdin_data,
    load_crossreference,
    load_portfolios,
    load_overrides,
    save_excel,
)

from scripts.utils.clarity_data_quality_control_functions import (
    prepare_dataframes,
    generate_delta,
    create_override_dict,
    add_portfolio_benchmark_info_to_df,
    get_issuer_level_df,
    filter_empty_lists,
    filter_rows_with_common_elements,
    reorder_columns,
    clean_inclusion_list,
    clean_portfolio_and_exclusion_list,
    clean_exclusion_list_with_ovr,
    clean_empty_exclusion_rows,
    process_data_by_strategy,
    log_df_head_compact,
    log_dict_compact,
)

# Import the centralized configuration
from scripts.utils.config import get_config

# CONFIG SCRIPT
# Get the common configuration for the Pre-OVR-Analysis script.
config = get_config(
    "00-pre-ovr-analysis", interactive=False, gen_output_dir=True, output_dir_dated=True
)
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
OVR_BETA_PATH = SRI_DATA_DIR / "overrides" / "overrides_db_beta.xlsx"
COMMITTEE_PATH = paths["COMMITTEE_PATH"]
# Define the output directory and file based on the configuration.
OUTPUT_DIR = config["OUTPUT_DIR"]
OUTPUT_FILE = OUTPUT_DIR / f"{DATE}_pre_ovr_analysis.xlsx"


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
    "str_006_sec",
    "str_007_sect",
    "art_8_basicos",
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
    "str_007_sect",
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


# define parser function
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="generate only the full pre-override analysis or also the simplified version"
    )

    # add argument so user can choose if it wants both versions of the pre-ovr analysis
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Do you want to generate simplified version of the pre-ovr analysis?",
    )
    # add argument so user can choose if it wants zombie analysis
    parser.add_argument(
        "--zombie",
        action="store_true",
        help="Do you want to generate zombie analysis?",
    )

    # add positional argument date to not work together with the get_date script
    parser.add_argument("--date", nargs="?", help="Date in YYYYMM format (positional)")

    return parser


# Define main function
def main(simple: bool = False, zombie: bool = False):
    logger.info(f"Starting pre-ovr-analysis for {DATE}.")
    logger.info(f"IT WILL RUN STRATEGY LEVEL ANALYSIS: {simple}")
    logger.info(f"IT WILL RUN ZOMBIE ANALYSIS: {zombie}")
    # 1.    LOAD DATA
    logger.info("\n\n\n1. LOADING DATA\n\n\n")
    # 1.1.  aladdin /brs data / perimeters
    logger.info("Loading BRS data")
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    log_df_head_compact(brs_carteras, df_name="brs_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    log_df_head_compact(brs_benchmarks, df_name="brs_benchmarks")
    crossreference = load_crossreference(CROSSREFERENCE_PATH)

    # remove duplicate and nan permid in crossreference
    logger.info("Removing duplicates and NaN values from crossreference")
    crossreference.drop_duplicates(subset=["permid"], inplace=True)
    crossreference.dropna(subset=["permid"], inplace=True)
    log_df_head_compact(crossreference, df_name="crossreference")

    # get BRS data at issuer level for becnhmarks without empty aladdin_id
    brs_carteras_issuerlevel = get_issuer_level_df(brs_carteras, "aladdin_id")
    log_df_head_compact(brs_carteras_issuerlevel, df_name="brs_carteras_issuerlevel")
    # get BRS data at issuer level for becnhmarks without empty aladdin_id
    brs_benchmarks_issuerlevel = get_issuer_level_df(brs_benchmarks, "aladdin_id")
    log_df_head_compact(
        brs_benchmarks_issuerlevel, df_name="brs_benchmarks_issuerlevel"
    )

    # 1.2.  clarity data
    logger.info("Loading clarity data")
    prep_old_clarity_df = load_clarity_data(DF_PREV_PATH, columns_to_read)
    log_df_head_compact(prep_old_clarity_df, df_name="old_clarity_df_with_overrides")
    prep_new_clarity_df = load_clarity_data(DF_NEW_PATH, columns_to_read)
    log_df_head_compact(prep_new_clarity_df, df_name="new_clarity_df_without_overrides")
    # let's rename columns in df_1 and df_2 using the rename_dict
    prep_old_clarity_df.rename(columns=rename_dict, inplace=True)
    prep_new_clarity_df.rename(columns=rename_dict, inplace=True)
    df_2_copy = prep_new_clarity_df.copy()
    # add aladdin_id to df_1 and df_2
    logger.info("Adding aladdin_id to clarity dfs")
    prep_old_clarity_df = prep_old_clarity_df.merge(
        crossreference[["permid", "aladdin_id"]], on="permid", how="left"
    )
    log_df_head_compact(prep_old_clarity_df, df_name="old_clarity_df_with_aladdin_id")
    prep_new_clarity_df = prep_new_clarity_df.merge(
        crossreference[["permid", "aladdin_id"]], on="permid", how="left"
    )
    log_df_head_compact(prep_new_clarity_df, df_name="new_clarity_df_with_aladdin_id")

    logger.info(
        f"previous clarity df's  rows: {prep_old_clarity_df.shape[0]}, new clarity df's rows: {prep_new_clarity_df.shape[0]}"
    )

    # 1.3.   ESG Team data: Overrides & Portfolios
    logger.info("Loading SRI Team Data: Overrides, Portfolios & Benchmark SRI Strategy")

    """
    We will test the pre-ovr analys with the overrides from the beta version of the overrides database
    Uncomment the following lines to use the regular version of the overrides database
    """

    # overrides = load_overrides(OVR_PATH)
    logger.info("Loading overrides data with beta version of the ovr db")
    overrides = load_overrides(
        OVR_PATH
    )  # changed to the original one just in case for the time being
    log_df_head_compact(overrides, df_name="overrides")
    # rename column brs_id to aladdin_id
    if "brs_id" in overrides.columns:
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
    # let's log first few key value pairs of the ovr_dict
    log_dict_compact(ovr_dict, dict_name="ovr_dict", n=2)

    # Load portfolios & benchmarks dicts
    (
        portfolio_dict,
        benchmark_dict,
    ) = load_portfolios(path_pb=BMK_PORTF_STR_PATH, path_committe=COMMITTEE_PATH)

    log_dict_compact(portfolio_dict, dict_name="portfolio_dict", n=2)
    log_dict_compact(benchmark_dict, dict_name="benchmark_dict", n=2)
    # START PRE-OVR ANALYSIS
    logger.info("\n\n\nStarting pre-ovr-analysis\n\n\n")
    # 2.    PREP DATA FOR ANALYSIS
    logger.info("\n\n\n2. PREPPEING DATA FOR DELTA GENERATION\n\n\n")
    # make sure that the values of of the columns delta_test_cols are strings and all uppercase and strip
    for col in delta_test_cols:
        prep_old_clarity_df[col] = prep_old_clarity_df[col].str.upper().str.strip()
        prep_new_clarity_df[col] = prep_new_clarity_df[col].str.upper().str.strip()
        brs_carteras_issuerlevel[col] = (
            brs_carteras_issuerlevel[col].str.upper().str.strip()
        )
        brs_benchmarks_issuerlevel[col] = (
            brs_benchmarks_issuerlevel[col].str.upper().str.strip()
        )

    # 2.2.  PREPARE DATA CLARITY LEVEL
    logger.info("\nPreparing dataframes for clarity level\n")
    (
        prep_old_clarity_df,
        prep_new_clarity_df,
        new_issuers_clarity,
        out_issuer_clarity,
    ) = prepare_dataframes(prep_old_clarity_df, prep_new_clarity_df)
    # reset index for new_issuers_clarity and out_issuer_clarity
    new_issuers_clarity.reset_index(inplace=True)
    out_issuer_clarity.reset_index(inplace=True)
    # drop isin from out_issuer_clarity and new_issuers_clarity
    out_issuer_clarity.drop(columns=["isin"], inplace=True)
    new_issuers_clarity.drop(columns=["isin"], inplace=True)
    log_df_head_compact(prep_old_clarity_df, df_name="prep_old_clarity_df", n=10)
    log_df_head_compact(prep_new_clarity_df, df_name="prep_new_clarity_df", n=10)
    log_df_head_compact(out_issuer_clarity, df_name="out_issuer_clarity", n=5)
    log_df_head_compact(new_issuers_clarity, df_name="new_issuers_clarity", n=5)

    # log size of new and missing issuers
    logger.info(
        f"Number of new issuers in Clarity's df: {new_issuers_clarity.shape[0]}"
    )
    logger.info(
        f"Number of missing issuers in Clarity's df: {out_issuer_clarity.shape[0]}"
    )

    # 2.3.  PREPARE DATA BRS LEVEL FOR PORTFOLIOS
    logger.info("\nPreparing dataframes for BRS Portfolio level\n")
    (
        prep_brs_df_ptf,
        prep_clarity_df_ptf,
        in_clarity_but_not_in_brs,
        in_brs_but_not_in_clarity,  # zombies?
    ) = prepare_dataframes(
        brs_carteras_issuerlevel, prep_new_clarity_df, target_index="aladdin_id"
    )
    log_df_head_compact(prep_brs_df_ptf, df_name="prep_brs_df_ptf", n=10)
    log_df_head_compact(prep_clarity_df_ptf, df_name="prep_clarity_df_ptf", n=10)
    log_df_head_compact(
        in_clarity_but_not_in_brs, df_name="in_clarity_but_not_in_brs", n=5
    )
    log_df_head_compact(
        in_brs_but_not_in_clarity, df_name="in_brs_but_not_in_clarity", n=5
    )

    # log size of new and missing issuers
    logger.info(
        f"Number issuers in clarity but not Aladdin: {in_clarity_but_not_in_brs.shape[0]}"
    )
    logger.info(
        f"Number issuers in Aladdin's Portfolios but not Clarity: {in_brs_but_not_in_clarity.shape[0]}"
    )

    # 2.4.  PREPARE DATA BENCHMARK BRS LEVEL
    logger.info("\nPreparing dataframes for BRS benchmarks level\n")
    (
        prep_brs_df_bmk,
        prep_clarity_df_bmk,
        in_clarity_but_not_in_brs_benchmarks,
        in_brs_benchmark_but_not_in_clarity,
    ) = prepare_dataframes(
        brs_benchmarks_issuerlevel, prep_new_clarity_df, target_index="aladdin_id"
    )
    log_df_head_compact(prep_brs_df_bmk, df_name="prep_brs_df_bmk", n=10)
    log_df_head_compact(prep_clarity_df_bmk, df_name="prep_clarity_df_bmk", n=10)
    log_df_head_compact(
        in_clarity_but_not_in_brs_benchmarks,
        df_name="in_clarity_but_not_in_brs_benchmarks",
        n=5,
    )
    log_df_head_compact(
        in_brs_benchmark_but_not_in_clarity,
        df_name="in_brs_benchmark_but_not_in_clarity",
        n=5,
    )

    # log size of new and missing issuers
    logger.info(
        f"Number issuers in clarity but not benchmarks: {in_clarity_but_not_in_brs_benchmarks.shape[0]}"
    )
    logger.info(
        f"Number issuers in Benchmarks but not Clarity: {in_brs_benchmark_but_not_in_clarity.shape[0]}"
    )

    # NEW CROSSREFERENCE HAS MULTIPLE ISSUER ID FOR A SINGLE PERMID
    logger.info(
        f"Checking index uniqueness: df1 index duplicates: {prep_old_clarity_df.index.duplicated().sum()}"
    )
    logger.info(
        f"Checking index uniqueness: df_2 index duplicates: {prep_new_clarity_df.index.duplicated().sum()}"
    )
    duplicated_rows_df_1 = prep_old_clarity_df[
        prep_old_clarity_df.index.duplicated(keep=False)
    ]
    if not duplicated_rows_df_1.empty:
        logger.warning(f"\n\n=======CHECK THIS OUT==========\n\n")
        logger.warning(f"Duplicated indexes found in df1:\n{duplicated_rows_df_1}")
    duplicated_rows_df_2 = prep_new_clarity_df[
        prep_new_clarity_df.index.duplicated(keep=False)
    ]
    if not duplicated_rows_df_2.empty:
        logger.warning(f"Duplicated indexes found in df1:\n{duplicated_rows_df_2}")
    # log index, issuer_name, and aladdin_id for the duplicated_rows_df1 and duplicated_rows_df_2
    if not duplicated_rows_df_1.empty:
        logger.warning("\nTHERE ARE THE DUPLICATED ISSUERS!!!!!!!\n")
        logger.warning(
            f"Duplicated rows in df_1:\n{duplicated_rows_df_1[['issuer_name','aladdin_id']].to_string(index=True)}"
        )
    if not duplicated_rows_df_2.empty:
        logger.warning(
            f"Duplicated rows in df_2:\n{duplicated_rows_df_2[['issuer_name', 'aladdin_id']].to_string(index=True)}"
        )
        sys.exit()

    # 3. GENERATE DELTAS
    logger.info("\n\n\n3. GENERATING DELTAS\n\n\n")
    logger.info("Start comparing the dataframes and building their deltas")
    delta_process_config = [
        {
            "delta_name": "delta_clarity",
            "compared_dfs": [prep_old_clarity_df, prep_new_clarity_df],
            "target_index": "permid",
            "excl_incl_dict": {
                "excl_dict": {
                    "df_name": "delta_ex_clarity",
                    "delta_analysis_str": "exclusion",
                    "condition_list": ["EXCLUDED"],
                    "filtering_col": "new_exclusion",
                    "dropping_cols": ["new_inclusion", "inclusion_list"],
                },
                "incl_dict": {
                    "df_name": "delta_in_clarity",
                    "delta_analysis_str": "inclusion",
                    "condition_list": ["OK", "FLAG"],
                    "filtering_col": "new_inclusion",
                    "dropping_cols": ["new_exclusion", "exclusion_list"],
                },
            },
        },
        {
            "delta_name": "delta_brs_ptf",
            "compared_dfs": [prep_brs_df_ptf, prep_clarity_df_ptf],
            "target_index": "aladdin_id",
            "excl_incl_dict": {
                "excl_dict": {
                    "df_name": "delta_ex_ptf",
                    "delta_analysis_str": "exclusion",
                    "condition_list": ["EXCLUDED"],
                    "filtering_col": "new_exclusion",
                    "dropping_cols": ["new_inclusion", "inclusion_list"],
                },
                "incl_dict": {
                    "df_name": "delta_in_ptf",
                    "delta_analysis_str": "inclusion",
                    "condition_list": ["OK", "FLAG"],
                    "filtering_col": "new_inclusion",
                    "dropping_cols": ["new_exclusion", "exclusion_list"],
                },
            },
        },
        {
            "delta_name": "delta_brs_bmks",
            "compared_dfs": [prep_brs_df_bmk, prep_clarity_df_bmk],
            "target_index": "aladdin_id",
            "excl_incl_dict": {
                "excl_dict": {
                    "df_name": "delta_ex_bmk",
                    "delta_analysis_str": "exclusion",
                    "condition_list": ["EXCLUDED"],
                    "filtering_col": "new_exclusion",
                    "dropping_cols": ["new_inclusion", "inclusion_list"],
                },
                "incl_dict": {
                    "df_name": "delta_in_bmk",
                    "delta_analysis_str": "inclusion",
                    "condition_list": ["OK", "FLAG"],
                    "filtering_col": "new_inclusion",
                    "dropping_cols": ["new_exclusion", "exclusion_list"],
                },
            },
        },
    ]

    deltas_df_dict = {}

    for config in delta_process_config:
        delta_name = f"{config["delta_name"]}"
        old_df, new_df = config["compared_dfs"]
        target_idx = config["target_index"]
        logger.info(f"Initiating delta generation process for {delta_name}")
        excl_df_name = config["excl_incl_dict"]["excl_dict"]["df_name"]
        excl_delta_analysis_str = config["excl_incl_dict"]["excl_dict"][
            "delta_analysis_str"
        ]
        excl_condition_list = config["excl_incl_dict"]["excl_dict"]["condition_list"]
        excl_filtering_col = config["excl_incl_dict"]["excl_dict"]["filtering_col"]
        excl_dropping_cols = config["excl_incl_dict"]["excl_dict"]["dropping_cols"]
        logger.info(f"Generating delta for {excl_df_name}")
        deltas_df_dict[excl_df_name] = generate_delta(
            old_df,
            new_df,
            delta_analysis_str=excl_delta_analysis_str,
            condition_list=excl_condition_list,
            get_inc_excl=True,
            delta_name_str=delta_name,
            target_index=target_idx,
            filter_col=excl_filtering_col,
            drop_cols=excl_dropping_cols,
        )
        incl_df_name = config["excl_incl_dict"]["incl_dict"]["df_name"]
        incl_delta_analysis_str = config["excl_incl_dict"]["incl_dict"][
            "delta_analysis_str"
        ]
        incl_condition_list = config["excl_incl_dict"]["incl_dict"]["condition_list"]
        incl_filtering_col = config["excl_incl_dict"]["incl_dict"]["filtering_col"]
        incl_dropping_cols = config["excl_incl_dict"]["incl_dict"]["dropping_cols"]
        logger.info(f"Generating delta for {excl_df_name}")
        deltas_df_dict[incl_df_name] = generate_delta(
            old_df,
            new_df,
            delta_analysis_str=incl_delta_analysis_str,
            condition_list=incl_condition_list,
            get_inc_excl=True,
            delta_name_str=delta_name,
            target_index=target_idx,
            filter_col=incl_filtering_col,
            drop_cols=incl_dropping_cols,
        )

    # Generate One Off Delta for new Flags
    delta_flagged = generate_delta(
        df1=prep_brs_df_ptf,
        df2=prep_clarity_df_ptf,
        condition_list=["FLAG"],
        delta_analysis_str="flagged",
        get_inc_excl=True,
        delta_name_str="delta_flagged",
        target_index="aladdin_id",
        filter_col="new_flagged",
        drop_cols=[],
    )

    # check if brs_carteras_issuerlevel & if aladdin_id is in columns
    # check if aladdin_id is index of brs_carteras_issuerlevel
    if "aladdin_id" == brs_carteras_issuerlevel.index.name:
        # resetindex
        brs_carteras_issuerlevel.reset_index(inplace=True)

    if "aladdin_id" not in brs_carteras_issuerlevel.columns:
        logger.error(
            "aladdin_id is not in brs_carteras_issuerlevel columns. Please check the data."
        )
        sys.exit(1)

    delta_flagged = delta_flagged.merge(
        brs_carteras_issuerlevel, how="left", on="aladdin_id", suffixes=("", "_brs")
    )
    delta_flagged = reorder_columns(
        delta_flagged,
        keep_first=["aladdin_id", "issuer_name"],
        exclude=[
            "isin",
            "issuer_name_brs",
            "security_description",
            "portfolio_full_name",
            "portfolio_id",
        ],
        keep_last=["flagged_list"],
    )

    logger.info(f"Delta Flagged df head:\n{delta_flagged.head()}")
    delta_flagged.to_csv(
        rf"C:\Users\n740789\Downloads\{DATE}_delta_flagged.csv", index=False
    )

    # logg to check dfs columns before prepping
    logger.info(
        "\n\n\n============DELTAS' SHAPE, COLUMNS, & HEAD  AFTER FILTERING & DROPPING=============\n\n\n"
    )

    for df_name, df in deltas_df_dict.items():
        # temp_pathout = OUTPUT_DIR / f"{DATE}_{df_name}_UNFILTERED.xlsx"
        # df.to_excel(temp_pathout, index=False)
        logger.info(
            f"{df_name}'s index:{df.index.name}, \n{df_name}'s shape {df.shape[0]} & \ncolumns:\n {df.columns.tolist()}\n\n"
        )
        # Temporarily override pandas display options
        with pd.option_context(
            "display.max_rows",
            None,
            "display.max_columns",
            None,
            "display.width",
            None,
            "display.max_colwidth",
            None,
        ):
            log_df_head_compact(df, df_name=df_name, n=10)

        # logg to check dfs columns before prepping
    logger.info("\n\n\n==========================================\n\n\n")

    # 3.1.  Unpack DELTAS
    # Unpacking filtered dataframes after filtering and dropping columns

    delta_ex_clarity = deltas_df_dict["delta_ex_clarity"].copy()
    delta_in_clarity = deltas_df_dict["delta_in_clarity"].copy()
    delta_ex_ptf = deltas_df_dict["delta_ex_ptf"].copy()
    delta_in_ptf = deltas_df_dict["delta_in_ptf"].copy()
    delta_ex_bmk = deltas_df_dict["delta_ex_bmk"].copy()
    delta_in_bmk = deltas_df_dict["delta_in_bmk"].copy()

    # log_df_head_compact(delta_ex_clarity, df_name="delta_exclustion_clarity", n=5)
    # log_df_head_compact(delta_in_clarity, df_name="delta_inclusion_clarity", n=5)
    # log_df_head_compact(delta_ex_ptf, df_name="delta_exclusion_portfolio", n=5)
    # log_df_head_compact(delta_in_ptf, df_name="delta_inclusion_portfolio", n=5)
    # log_df_head_compact(delta_ex_bmk, df_name="delta_exclusion_benchmark", n=5)
    # log_df_head_compact(delta_in_bmk, df_name="delta_inclusion_benchmark", n=5)

    # Free space by delting the dicts and config list you are done with
    del deltas_df_dict, delta_process_config

    # 4.    ADDING PORTFOLIO & BENCHMARK INFO TO DELTAS & FILTERING & CLEANING
    # Define prepping configuration
    prep_config = [
        {
            "prep_config_name": "clarity_deltas",
            "dfs_dict": {
                "exclusion_df": delta_ex_clarity,
                "inclusion_df": delta_in_clarity,
            },
            "check_permid": False,
            "brs_data": False,
        },
        {
            "prep_config_name": "portfolio_deltas",
            "dfs_dict": {"exclusion_df": delta_ex_ptf, "inclusion_df": delta_in_ptf},
            "check_permid": True,
            "brs_data": True,
            "main_parameter": "affected_portfolio_str",
        },
        {
            "prep_config_name": "benchmark_deltas",
            "dfs_dict": {"exclusion_df": delta_ex_bmk, "inclusion_df": delta_in_bmk},
            "check_permid": True,
            "brs_data": True,
            "main_parameter": "affected_benchmark_str",
        },
    ]

    final_dfs_dict = {}  # to persist all cleaned dataframes

    # DEBUG LOGGING - REMOVE LATER - LET'S CHECK HOW THE DATA LOOKS LIKE
    # Populate final_dfs_dict with the initial dfs_dicts
    for config in prep_config:
        for df_name, df in config["dfs_dict"].items():
            final_key = f"{config['prep_config_name']}_{df_name}"
            final_dfs_dict[final_key] = df

    logger.info(
        "\n\n========== SHOW dataframes BEFORE Adding Portfolio & Benchmark Information, Filtering, and Cleaning ==========\n\n"
    )
    for df_name, df in final_dfs_dict.items():

        # logg columns for all the dfs
        logger.info(f"Columns in {df_name}:\n {df.columns.tolist()}\n")
        # Log if df has index and if it does the index name
        if df.index.name:
            logger.info(f"Index name for {df_name}: {df.index.name}")
            logger.info(
                f"""
            Columns in {df_name}:\n {df.columns.tolist()}\n
            Number of rows in {df_name}: {df.shape[0]}\n
            """
            )
            # Temporarily override pandas display options
            with pd.option_context(
                "display.max_rows",
                None,
                "display.max_columns",
                None,
                "display.width",
                None,
                "display.max_colwidth",
                None,
            ):
                logger.info(f"{df_name}'s head:\n{df.head()}\n\n")

        else:
            logger.info(
                f"""
            Columns in {df_name}:\n {df.columns.tolist()}\n
            Number of rows in {df_name}: {df.shape[0]}\n
            """
            )
            # Temporarily override pandas display options
            with pd.option_context(
                "display.max_rows",
                None,
                "display.max_columns",
                None,
                "display.width",
                None,
                "display.max_colwidth",
                None,
            ):
                logger.info(f"{df_name}'s head:\n{df.head()}\n\n")

    logger.info("\n\n================================================\n\n")

    logger.info("\n\n\n4.  ADDING PORTFOLIO & BENCHMARK INFO TO DELTAS\n\n\n")

    for config in prep_config:
        logger.info(
            "\n4.1. standarise indeces, drop column isin if present, and add ovr_list to all dfs_dict"
        )
        # 4.1. standarise indeces, drop column isin if present, and add ovr_list to all dfs_dict
        for df_name, df in config["dfs_dict"].items():
            # Standarise Indices & Reset Index if necessary
            logger.info("Let's standrise indeces")
            if "index" in df.columns:
                df.drop(columns="index", inplace=True)
                logger.info(
                    f"Dropped column 'index' from {config["prep_config_name"]}'s {df_name}."
                )

            # Reset index if it's aladdin_id or permid
            if df.index.name in ["aladdin_id", "permid"]:
                df.reset_index(inplace=True)
                logger.info(
                    f"Reset index from {config["prep_config_name"]}'s {df_name}."
                )
            # let's drop isin from all dfs_dict if it's a column
            if "isin" in df.columns:
                logger.info(f"Dropping isin column from {df_name}")
                df.drop(columns=["isin"], inplace=True, errors="ignore")

            # add new column 'ovr_list' value using aladdin_id
            logger.info(
                f"Adding column 'ovr_list' to {config["prep_config_name"]}'s {df_name}"
            )
            try:
                df["ovr_list"] = df["aladdin_id"].map(ovr_dict)
                config["dfs_dict"][df_name] = df
            except KeyError as e:
                e.add_note(
                    f"Missing expected colummn in {config["prep_config_name"]}'s {df_name}."
                )
                e.add_note(
                    f"Colummn in {config["prep_config_name"]}'s {df_name}:\n{df.columns.tolist()}"
                )
                logger.error(f"{e}")
                raise e

        # 4.2. check for dataframes that do not have natively permid that they have the column
        logger.info(
            "\n4.2. check for dataframes that do not have natively permid that they have the column"
        )
        if config["check_permid"]:
            for df_name, df in config["dfs_dict"].items():
                # check if permid is in df.columns - if missing merge using crossreference
                if "permid" not in df.columns:
                    logger.info(f"permid not in {df_name}")
                    logger.info(
                        f"Merging to add permid to {config["prep_config_name"]}'s {df_name}"
                    )
                    df = df.merge(
                        crossreference[["aladdin_id", "permid"]],
                        on="aladdin_id",
                        how="left",
                    )
                    config["dfs_dict"][df_name] = df
                else:
                    logger.info(f"permid already in {df_name}, continue...")
        else:
            logger.info(f"permid already in {df_name}, continue...")

        # 4.3. Adding portfolio and benchmark information to dfs
        logger.info("\n4.3. Adding portfolio and benchmark information to dfs")
        for df_name, df in config["dfs_dict"].items():
            # 4.3.1. Add affected Portfolio info to df
            logger.info(
                f" Adding affect portfolio info to {config["prep_config_name"]}'s {df_name}"
            )
            df = add_portfolio_benchmark_info_to_df(portfolio_dict, df)
            # 4.3.2. Add affected Benchmark info to df )
            logger.info(
                f" Adding affect benchmark info to {config["prep_config_name"]}'s {df_name}"
            )
            # 4.3.3. filter_non_empty_lists
            df = add_portfolio_benchmark_info_to_df(
                benchmark_dict, df, "affected_benchmark_str"
            )
            # safe updated df into the dict
            config["dfs_dict"][df_name] = df

    # DEBUG LOGGING - REMOVE LATER - LET'S CHECK HOW THE DATA LOOKS LIKE
    # Populate final_dfs_dict with the initial dfs_dicts
    for config in prep_config:
        for df_name, df in config["dfs_dict"].items():
            final_key = f"{config['prep_config_name']}_{df_name}"
            final_dfs_dict[final_key] = df

    logger.info(
        "\n\n========== SHOW dataframes AFTER Adding Portfolio & Benchmark Information. BUT BEFORE Filtering, and Cleaning ==========\n\n"
    )
    for df_name, df in final_dfs_dict.items():

        # logg columns for all the dfs
        logger.info(f"Columns in {df_name}:\n {df.columns.tolist()}\n")
        # Log if df has index and if it does the index name
        if df.index.name:
            logger.info(f"Index name for {df_name}: {df.index.name}")
            logger.info(
                f"""
            Columns in {df_name}:\n {df.columns.tolist()}\n
            Number of rows in {df_name}: {df.shape[0]}\n
            """
            )
            # Temporarily override pandas display options
            with pd.option_context(
                "display.max_rows",
                None,
                "display.max_columns",
                None,
                "display.width",
                None,
                "display.max_colwidth",
                None,
            ):
                log_df_head_compact(df, df_name=df_name, n=10)

        else:
            logger.info(
                f"""
            Columns in {df_name}:\n {df.columns.tolist()}\n
            Number of rows in {df_name}: {df.shape[0]}\n
            """
            )
            # Temporarily override pandas display options
            with pd.option_context(
                "display.max_rows",
                None,
                "display.max_columns",
                None,
                "display.width",
                None,
                "display.max_colwidth",
                None,
            ):
                log_df_head_compact(df, df_name=df_name, n=10)

    logger.info("\n\n================================================\n\n")

    # 5.    FILTERING & CLEANING DELTAS
    logger.info("\n\n\n5. FILTERING & CLEANING DELTAS\n\n\n")

    for config in prep_config:
        # 5. Filtering BRS Delta's based on affected portfolio & benchmark
        logger.info(
            "\n5. Filtering & Cleaning BRS Delta's based on affected portfolio & benchmark\n"
        )
        if config["brs_data"]:

            for df_name, df in config["dfs_dict"].items():
                logger.info(
                    f"\n5.1 Filtering Empty {config["main_parameter"]} for {config["prep_config_name"]}'s {df_name}.\n"
                )
                # 5.1. filtering not empty lists
                logger.info(
                    f"""
                    {df_name} had {df.shape[0]} before applying filter_empty_lists() func to empty {config["main_parameter"]}.
                    """
                )
                df = filter_empty_lists(df, config["main_parameter"])
                config["dfs_dict"][df_name] = df
                logger.info(
                    f"""
                    {df_name} has {df.shape[0]} after applying filter_empty_lists() func to empty {config["main_parameter"]}.
                    """
                )

        # 5.2.a filter rows with common elements
        if config["brs_data"]:
            for df_name, df in config["dfs_dict"].items():
                if df_name == "exclusion_df":
                    # 5.2.1.a filter rows with common elements in the exclusion list & clean exclusion list
                    logger.info(
                        f"""
                        \n5.2.1.a Filtering rows with common elements using filter_rows_with_common_elements() func for {config["prep_config_name"]}'s {df_name}.
                        {df_name} had {df.shape[0]} ROWS before applying filter_rows_with_common_elements() func to empty {config["main_parameter"]}.
                        """
                    )
                    df = filter_rows_with_common_elements(
                        df, "exclusion_list", config["main_parameter"]
                    )
                    logger.info(
                        f"""
                        {df_name} has {df.shape[0]} ROWS after applying filter_rows_with_common_elements() func to empty {config["main_parameter"]}.
                        """
                    )
                    # 5.2.2.a Clean exclusion data
                    logger.info(
                        f"""
                        \n5.2.2.a Cleaning exclusion list for overrides OK. 
                        {df_name} had {df.shape[0]} rows BEFORE applying clean_exclusion_list_with_ovr() func.
                        """
                    )
                    df = clean_exclusion_list_with_ovr(df)
                    config["dfs_dict"][df_name] = df
                    logger.info(
                        f"{df_name} has {config["dfs_dict"][df_name].shape[0]} rows AFTER applying clean_exclusion_list_with_ovr() func."
                    )

                else:
                    # 5.2.b filter rows with common elements in the inclusion list & clean inclusion list
                    # 5.2.1.b filter rows with common elements in the exclusion list & clean exclusion list
                    logger.info(
                        f"""
                        \n5.2.1.b Filter rows with common elements in the inclusion list & clean inclusion list. 
                        {df_name} had {config["dfs_dict"][df_name].shape[0]} rows BEFORE applying filter_rows_with_common_elements() func.
                        """
                    )

                    df = filter_rows_with_common_elements(
                        df, "inclusion_list", config["main_parameter"]
                    )
                    # 5.2.2.b Clean inclusion data
                    logger.info(
                        f"""
                        \n5.2.2.b Cleaning inclusion list. 
                        {df_name} had {config["dfs_dict"][df_name].shape[0]} rows BEFORE applying clean_inclusion_list() func.
                        """
                    )
                    df = clean_inclusion_list(df)
                    config["dfs_dict"][df_name] = df
                    logger.info(
                        f"{df_name} has {config["dfs_dict"][df_name].shape[0]} rows AFTER applying clean_inclusion_list() func.\n"
                    )

        # 5.3 apply clean portfolio and exclusion list
        if config["prep_config_name"] == "portfolio_deltas":
            logger.info(
                "\n5.3 Cleaning portfolio and exclusion lists for BRS Portfolio Delta"
            )
            prep_config_name = config["prep_config_name"]
            for df_name, df in config["dfs_dict"].items():
                if df_name == "exclusion_df":

                    # cleant portfolio and exclusion list
                    logger.info(
                        f"""
                        {prep_config_name}'s {df_name} had {df.shape[0]} rows BEFORE applying clean_portfolio_and_exclusion_list() func.
                        """
                    )
                    df = df.apply(clean_portfolio_and_exclusion_list, axis=1)

                    logger.info(
                        f"""
                        {prep_config_name}'s {df_name} had {df.shape[0]} rows AFTER applying clean_portfolio_and_exclusion_list() func.
                        """
                    )
                    config["dfs_dict"][df_name] = df

        # 5.4. clean empty exclusion lists
        for df_name, df in config["dfs_dict"].items():
            prep_config_name = config["prep_config_name"]
            # remove rows with empyt exclusion lists
            if df_name == "exclusion_df":
                logger.info(
                    f"""
                            \n5.4. Cleaning empty exclusion lists for {prep_config_name}'s {df_name}.
                            {prep_config_name}'s {df_name} had {df.shape[0]} rows BEFORE applying clean_empty_exclusion_rows() func.
                            """
                )
                df = clean_empty_exclusion_rows(df)
                config["dfs_dict"][df_name] = df
                logger.info(
                    f"""
                            {prep_config_name}'s {df_name} had {df.shape[0]} rows AFTER applying clean_empty_exclusion_rows() func.
                            """
                )

        for df_name, df in config["dfs_dict"].items():
            logger.debug(
                "Columns *before* reorder %s / %s: %s",
                config["prep_config_name"],
                df_name,
                list(df.columns),
            )

        # 5.5. reoder columns for all the deltas
        logger.info("\n6. Reordering columns for all the deltas")
        for df_name, df in config["dfs_dict"].items():
            logger.info(
                f"Reordering columns for {config["prep_config_name"]}'s {df_name}"
            )
            df = reorder_columns(
                df=df, keep_first=id_name_issuers_cols, exclude=delta_test_cols
            )
            config["dfs_dict"][df_name] = df

    # persist cleaned DataFrames
    for config in prep_config:
        for df_name, df in config["dfs_dict"].items():
            final_key = f"{config['prep_config_name']}_{df_name}"
            final_dfs_dict[final_key] = df

    # DEBUG LOGGING - REMOVE LATER - LET'S CHECK HOW THE DATA LOOKS LIKE
    logger.info(
        "\n\n========== SHOW dataframes AFTER Adding Portfolio & Benchmark Information & Filtering, and Cleaning ==========\n\n"
    )
    for df_name, df in final_dfs_dict.items():

        # logg columns for all the dfs
        logger.info(
            f"""
        Columns in {df_name}:\n {df.columns.tolist()}\n
        Number of rows in {df_name}: {df.shape[0]}\n
        """
        )
        # Log if df has index and if it does the index name
        if df.index.name:
            logger.info(f"Index name for {df_name}: {df.index.name}")
            logger.info(
                f"""
            Columns in {df_name}:\n {df.columns.tolist()}\n
            Number of rows in {df_name}: {df.shape[0]}\n
            """
            )
            # Temporarily override pandas display options
            with pd.option_context(
                "display.max_rows",
                None,
                "display.max_columns",
                None,
                "display.width",
                None,
                "display.max_colwidth",
                None,
            ):
                log_df_head_compact(df, df_name=df_name, n=10)

        else:
            logger.info(
                f"""
            Columns in {df_name}:\n {df.columns.tolist()}\n
            Number of rows in {df_name}: {df.shape[0]}\n
            """
            )
            # Temporarily override pandas display options
            with pd.option_context(
                "display.max_rows",
                None,
                "display.max_columns",
                None,
                "display.width",
                None,
                "display.max_colwidth",
                None,
            ):
                log_df_head_compact(df, df_name=df_name, n=10)

    logger.info("\n\n================================================\n\n")

    # Unpack cleaned DataFrames using original names
    delta_ex_clarity = final_dfs_dict["clarity_deltas_exclusion_df"].copy()
    delta_in_clarity = final_dfs_dict["clarity_deltas_inclusion_df"].copy()
    delta_ex_ptf = final_dfs_dict["portfolio_deltas_exclusion_df"].copy()
    delta_in_ptf = final_dfs_dict["portfolio_deltas_inclusion_df"].copy()
    delta_ex_bmk = final_dfs_dict["benchmark_deltas_exclusion_df"].copy()
    delta_in_bmk = final_dfs_dict["benchmark_deltas_inclusion_df"].copy()

    # Free up memory: delete prep structure and final dict
    del prep_config, final_dfs_dict

    # 6. GET STRATEGIES DFS
    if simple:
        logger.info("\n\n\n6. GETTING STRATEGIES DFS\n\n\n")
        logger.info("Getting strategies dfs")

        configurations = [
            {
                "description": "Exclusion BRS Exclusion Analysis at the Portfolio level",
                "var_name": "strategy_level_preovr_analysis_excl_ptf",
                "input_df": delta_ex_ptf,
                "exclusion_col": "exclusion_list",
                "affected_col": "affected_portfolio_str",
                "brs_source": prep_brs_df_ptf,
            },
            {
                "description": "Exclusion BRS Exclusion Analysis at the Benchmark level",
                "var_name": "strategy_level_preovr_analysis_excl_bmk",
                "input_df": delta_ex_bmk,
                "exclusion_col": "exclusion_list",
                "affected_col": "affected_benchmark_str",
                "brs_source": prep_brs_df_bmk,
            },
            {
                "description": "Exclusion BRS Exclusion Analysis at the Benchmark level",
                "var_name": "strategy_level_preovr_analysis_excl_clarity",
                "input_df": delta_ex_clarity,
                "exclusion_col": "exclusion_list",
                "affected_col": "affected_benchmark_str",
                "brs_source": prep_brs_df_bmk,
            },
            {
                "description": "Exclusion BRS Inclusion Analysis at the Portfolio level",
                "var_name": "strategy_level_preovr_analysis_incl_ptf",
                "input_df": delta_in_ptf,
                "exclusion_col": "inclusion_list",
                "affected_col": "affected_portfolio_str",
                "brs_source": prep_brs_df_ptf,
            },
            {
                "description": "Exclusion BRS Inclusion Analysis at the Benchmark level",
                "var_name": "strategy_level_preovr_analysis_incl_bmk",
                "input_df": delta_in_bmk,
                "exclusion_col": "inclusion_list",
                "affected_col": "affected_benchmark_str",
                "brs_source": prep_brs_df_bmk,
            },
            {
                "description": "Exclusion BRS Inclusion Analysis at the Benchmark level",
                "var_name": "strategy_level_preovr_analysis_incl_clarity",
                "input_df": delta_in_clarity,
                "exclusion_col": "inclusion_list",
                "affected_col": "affected_benchmark_str",
                "brs_source": prep_brs_df_bmk,
            },
        ]

        results_str_level_dfs = {}

        for config in configurations:
            logger.info(f"Getting {config['description']}")
            results_str_level_dfs[config["var_name"]] = process_data_by_strategy(
                input_delta_df=config["input_df"],
                strategies_list=delta_test_cols,
                input_df_exclusion_col=config["exclusion_col"],
                df1_lookup_source=prep_old_clarity_df,
                df2_lookup_source=prep_new_clarity_df,
                brs_lookup_source=config["brs_source"],
                overrides_df=overrides,
                affected_portfolio_col_name=config["affected_col"],
                logger=logger,
            )
    else:
        pass

    # 7.   Get Zombie Analysis
    if zombie:
        from scripts.utils.zombie_killer import main as zombie_killer

        logger.info("\n\n\n6. GENERATING ZOMBIE ANALYSIS df\n\n\n")
        zombie_df = zombie_killer(
            clarity_df=df_2_copy,
            brs_carteras=brs_carteras,
            brs_benchmarks=brs_benchmarks,
            crosreference=crossreference,
        )
    else:
        pass

    # 8. SAVE INTO EXCEL
    logger.info("\n\n\n7. SAVING DATA INTO EXCEL\n\n\n")

    # create dict of df and df name
    dfs_dict = {
        "excl_carteras": delta_ex_ptf,
        "excl_benchmarks": delta_ex_bmk,
        "excl_clarity": delta_ex_clarity,
        "incl_clarity": delta_in_clarity,
        "incl_carteras": delta_in_ptf,
        "incl_benchmarks": delta_in_bmk,
    }
    if zombie:
        dfs_dict["zombie_analysis"] = zombie_df
    else:
        pass

    # save to excel
    if simple:
        for outer_key, inner_dict in results_str_level_dfs.items():
            if not isinstance(inner_dict, dict):
                logger.warning(f"{outer_key} is not a dictionary, skipping.")
                continue

            for inner_key, maybe_df in inner_dict.items():
                df_name = f"{outer_key}.{inner_key}"

                # Only proceed if it's an actual DataFrame and not empty
                if isinstance(maybe_df, pd.DataFrame):
                    if maybe_df.empty:
                        logger.info(f"{df_name} is empty, skipping.")
                    else:
                        log_df_head_compact(maybe_df, df_name=df_name, n=5)
                else:
                    logger.warning(
                        f"{df_name} is not a DataFrame: {type(maybe_df).__name__}"
                    )
        for key, df in results_str_level_dfs.items():
            save_excel(df, OUTPUT_DIR, file_name=f"{DATE}_{key}")
            logger.info(
                f"\nSaved {key} to {OUTPUT_DIR}/{DATE}_{key}_preovr_analysis.xlsx\n"
            )
        save_excel(dfs_dict, OUTPUT_DIR, file_name=f"{DATE}_preovr_analysis")
        logger.info(f"\nSaved dfs_dict to {OUTPUT_DIR}/{DATE}_preovr_analysis.xlsx\n")

    else:
        log_df_head_compact(df, df_name=f"{DATE}_preovr_analysis", n=5)
        save_excel(dfs_dict, OUTPUT_DIR, file_name=f"{DATE}_preovr_analysis")
        logger.info(f"\nSaved dfs_dict to {OUTPUT_DIR}/{DATE}_preovr_analysis.xlsx\n")


if __name__ == "__main__":
    # check if user wants also the simplified ovr analysis
    args = parse_arguments().parse_args()
    if args.simple:
        # generate simplify over analysis
        main(simple=True)
        logger.info("\n\n\n FINISHED PRE-OVR ANALYSIS\n\n\n")
    else:
        main()
        logger.info("\n\n\n FINISHED PRE-OVR ANALYSIS\n\n\n")
