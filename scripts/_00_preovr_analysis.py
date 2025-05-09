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
    clean_inclusion_list,
    clean_portfolio_and_exclusion_list,
    clean_exclusion_list_with_ovr,
    clean_empty_exclusion_rows,
    process_data_by_strategy,
    filter_and_drop,
)

from scripts.utils.zombie_killer import main as zombie_killer

# Import the centralized configuration
from scripts.utils.config import get_config

# CONFIG SCRIPT
# Get the common configuration for the Pre-OVR-Analysis script.
config = get_config(
    "pre-ovr-analysis", interactive=False, gen_output_dir=True, output_dir_dated=True
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

    # 1.1.  aladdin /brs data / perimeters
    logger.info("Loading BRS data")
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    crosreference = load_crossreference(CROSSREFERENCE_PATH)

    # remove duplicate and nan permid in crossreference
    logger.info("Removing duplicates and NaN values from crossreference")
    crosreference.drop_duplicates(subset=["permid"], inplace=True)
    crosreference.dropna(subset=["permid"], inplace=True)

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
    df_2_copy = df_2.copy()
    # add aladdin_id to df_1 and df_2
    logger.info("Adding aladdin_id to clarity dfs")
    df_1 = df_1.merge(crosreference[["permid", "aladdin_id"]], on="permid", how="left")
    df_2 = df_2.merge(crosreference[["permid", "aladdin_id"]], on="permid", how="left")

    logger.info(
        f"previous clarity df's  rows: {df_1.shape[0]}, new clarity df's rows: {df_2.shape[0]}"
    )

    # 1.3.   ESG Team data: Overrides & Portfolios
    logger.info("Loading SRI Team Data: Overrides, Portfolios & Benchmark SRI Strategy")

    """
    We will test the pre-ovr analys with the overrides from the beta version of the overrides database
    Uncomment the following lines to use the regular version of the overrides database
    """

    # overrides = load_overrides(OVR_PATH)
    logger.info("Loading overrides data with beta version of the ovr db")
    overrides = load_overrides(OVR_BETA_PATH)
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
        brs_df_portfolios,
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

    # NEW CROSSREFERENCE HAS MULTIPLE ISSUER ID FOR A SINGLE PERMID
    logger.info(
        f"Checking index uniqueness: df1 index duplicates: {df_1.index.duplicated().sum()}"
    )
    logger.info(
        f"Checking index uniqueness: df_2 index duplicates: {df_2.index.duplicated().sum()}"
    )
    duplicated_rows_df_1 = df_1[df_1.index.duplicated(keep=False)]
    if not duplicated_rows_df_1.empty:
        logger.warning(f"\n\n=======CHECK THIS OUT==========\n\n")
        logger.warning(f"Duplicated indexes found in df1:\n{duplicated_rows_df_1}")
    duplicated_rows_df_2 = df_2[df_2.index.duplicated(keep=False)]
    if not duplicated_rows_df_2.empty:
        logger.warning(f"Duplicated indexes found in df1:\n{duplicated_rows_df_2}")
    # log index, issuer_name, and aladdin_id for the duplicated_rows_df1 and duplicated_rows_df_2
    if not duplicated_rows_df_1.empty:
        logger.warning("\nHERE ARE THE DUPLICATED ISSUERS!!!!!!!\n")
        logger.warning(
            f"Duplicated rows in df_1:\n{duplicated_rows_df_1[['issuer_name','aladdin_id']].to_string(index=True)}"
        )
    if not duplicated_rows_df_2.empty:
        logger.warning(
            f"Duplicated rows in df_2:\n{duplicated_rows_df_2[['issuer_name', 'aladdin_id']].to_string(index=True)}"
        )
        sys.exit()

    # COMPARE DATA

    logger.info("Start comparing the dataframes and building their deltas")
    compare_data_config = [
        {
            "delta_name": "delta_clarity",
            "compared_dfs": [df_1, df_2],
            "suffix": "",
            "target_index": "permid",
        },
        {
            "delta_name": "delta_brs_ptf",
            "compared_dfs": [brs_df_portfolios, clarity_df],
            "suffix": "_brs",
            "target_index": "aladdin_id",
        },
        {
            "delta_name": "delta_brs_bmks",
            "compared_dfs": [brs_df_benchmarks, clarity_df_benchmarks],
            "suffix": "_brs",
            "target_index": "aladdin_id",
        },
    ]

    deltas_df_dict = {}

    for config in compare_data_config:
        delta_name = f"{config["delta_name"]}"
        logger.info(f"Initiating delta {delta_name}")
        old_df, new_df = config["compared_dfs"]
        deltas_df_dict[delta_name] = compare_dataframes(old_df, new_df)

    for config in compare_data_config:
        delta_name = f"{config["delta_name"]}"
        logger.info(f"Checking new exclusions and inclusions for {delta_name}")
        old_df, new_df = config["compared_dfs"]
        suffix = config["suffix"]
        target_idx = config["target_index"]
        delta_df = deltas_df_dict[delta_name]
        delta_df = check_new_exclusions(old_df, new_df, delta_df, suffix_level=suffix)
        delta_df = check_new_inclusions(old_df, new_df, delta_df, suffix_level=suffix)
        delta_df = finalize_delta(delta_df, target_index=target_idx)
        deltas_df_dict[delta_name] = delta_df

    # logg to check dfs columns before prepping
    logger.info(
        "\n\n\n============DFS SHAPE AND COLUMNS BEFORE PREPRING=============\n\n\n"
    )

    for df_name, df in deltas_df_dict.items():
        logger.info(
            f"{df_name}'s index:{df.index.name} & columns:\n {df.columns.tolist()}\n\n"
        )

    # Define parameters for each filtering operation
    logger.info(
        "Let us now filter and drop inclusion and exclusion columnas and created dfs of ex/in both for ptf & bmk"
    )
    filter_configs = [
        (
            "delta_brs_ptf",
            "new_exclusion",
            ["new_exclusion", "new_inclusion", "inclusion_list_brs"],
            "delta_ex_ptf",
        ),
        (
            "delta_brs_ptf",
            "new_inclusion",
            ["new_exclusion", "new_inclusion", "exclusion_list_brs"],
            "delta_in_ptf",
        ),
        (
            "delta_brs_bmks",
            "new_exclusion",
            ["new_exclusion", "new_inclusion", "inclusion_list_brs"],
            "delta_ex_bmk",
        ),
        (
            "delta_brs_bmks",
            "new_inclusion",
            ["new_exclusion", "new_inclusion", "exclusion_list_brs"],
            "delta_in_bmk",
        ),
    ]

    # Use dictionary unpacking to store the results if needed
    filter_dfs_dict = {}

    for df_key, filter_col, drop_cols, result_key in filter_configs:
        df = deltas_df_dict[df_key]
        logger.info(f"Filtering and dropping column in df {df_key}")
        filter_dfs_dict[result_key] = filter_and_drop(df, filter_col, drop_cols, logger)

    # Unpacking filtered dataframes after filtering and dropping columns
    delta_ex_ptf = filter_dfs_dict["delta_ex_ptf"].copy()
    delta_in_ptf = filter_dfs_dict["delta_in_ptf"].copy()
    delta_ex_bmk = filter_dfs_dict["delta_ex_bmk"].copy()
    delta_in_bmk = filter_dfs_dict["delta_in_bmk"].copy()

    delta_clarity = deltas_df_dict["delta_clarity"].copy()

    # Free space by delting the dicts and config list you are done with
    del filter_dfs_dict, deltas_df_dict, compare_data_config, filter_configs

    # 3.    PREP DELTAS BEFORE SAVING
    logger.info("\nPreparing filtered deltas before saving")
    # Define prepping configuration
    prep_config = [
        {
            "prep_config_name": "clarity_deltas",
            "dfs_dict": {"delta_clarity": delta_clarity},
            "missing_permid": False,
            # let's define paramst for the battery of functions
            "brs_data": False,
        },  # clarity deltas
        {
            "prep_config_name": "portfolio_deltas",
            "dfs_dict": {"exclusion_df": delta_ex_ptf, "inclusion_df": delta_in_ptf},
            "missing_permid": True,
            # let's define paramst for the battery of functions
            "brs_data": True,
            "main_parameter": "affected_portfolio_str",
        },  # portfolio deltas
        {
            "prep_config_name": "benchmark_deltas",
            "dfs_dict": {"exclusion_df": delta_ex_bmk, "inclusion_df": delta_in_bmk},
            "missing_permid": True,
            # let's define paramst for the battery of functions
            "brs_data": True,
            "main_parameter": "affected_benchmark_str",
        },  # benchmark deltas
    ]

    final_dfs_dict = {}  # to persist all cleaned dataframes

    for config in prep_config:
        # apply to all deltdf_name, a dfs_dicts
        for df_name, df in config["dfs_dict"].items():
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
            # let's drop isin from all dfs_dict
            logger.info(f"Dropping isin column from {df_name}")
            df.drop(columns=["isin"], inplace=True, errors="ignore")
            # add new column 'ovr_dict' value using aladdin_id
            logger.info(
                f"Adding column 'ovr_list' to {config["prep_config_name"]}'s {df_name}"
            )
            try:
                df["ovr_list"] = df["aladdin_id"].map(ovr_dict)
                config["dfs_dict"][df_name] = df
            except KeyError as e:
                logger.error(
                    f"Missing expected colummn in {config["prep_config_name"]}'s {df_name}:\n{e}"
                )
                logger.info(
                    f"Colummn in {config["prep_config_name"]}'s {df_name}:\n{df.columns.tolist()}"
                )

        if config["missing_permid"]:
            for df_name, df in config["dfs_dict"].items():
                logger.info(
                    f"Merging to add permid to {config["prep_config_name"]}'s {df_name}"
                )
                df = df.merge(
                    crosreference[["aladdin_id", "permid"]], on="aladdin_id", how="left"
                )
                config["dfs_dict"][df_name] = df

        # let's apply the following functions:
        for df_name, df in config["dfs_dict"].items():
            # 1. add_portfolio_benchmark_info_to_df (without string "affected_benchmark_str")
            logger.info(
                f" Adding affect portfolio info to {config["prep_config_name"]}'s {df_name}"
            )
            df = add_portfolio_benchmark_info_to_df(portfolio_dict, df)
            # 2. add_portfolio_benchmark_info_to_df (with string "affected_benchmark_str")
            logger.info(
                f" Adding affect benchmark info to {config["prep_config_name"]}'s {df_name}"
            )
            df = add_portfolio_benchmark_info_to_df(
                benchmark_dict, df, "affected_benchmark_str"
            )
            # 3. filter_non_empty_lists
            config["dfs_dict"][df_name] = df

        if config["brs_data"]:
            for df_name, df in config["dfs_dict"].items():
                logger.info(
                    f"Filtering non empty lists for {config["prep_config_name"]}'s {df_name}"
                )
                df = filter_non_empty_lists(df, config["main_parameter"])
                # 4. filter_rows_with_common_elements
                logger.info(
                    f"Filtering rows with common elements for {config["prep_config_name"]}'s {df_name}"
                )
                if df_name == "exclusion_df":
                    df = filter_rows_with_common_elements(
                        df, "exclusion_list_brs", config["main_parameter"]
                    )
                    config["dfs_dict"][df_name] = df
                    logger.info(
                        f"{df_name} has {config["dfs_dict"][df_name].shape[0]} rows"
                    )
                else:
                    df = filter_rows_with_common_elements(
                        df, "inclusion_list_brs", config["main_parameter"]
                    )
                    config["dfs_dict"][df_name] = df
                    logger.info(
                        f"{df_name} has {config["dfs_dict"][df_name].shape[0]} rows"
                    )

        # 5. reoder columns for all the deltas
        for df_name, df in config["dfs_dict"].items():
            logger.info(
                f"Reordering columns for {config["prep_config_name"]}'s {df_name}"
            )
            df = reorder_columns(df, id_name_issuers_cols, delta_test_cols)
            if df_name == "exclusion_df":
                # Clean exclusion data
                logger.info("Cleaning exclusion list for overrides OK")
                df = clean_exclusion_list_with_ovr(df)
            elif df_name == "inclusion_df":
                # Clean inclusion data
                logger.info("Cleaning inclusion lists with overrides")
                df = clean_inclusion_list(df)
            else:
                logger.info(
                    f"Not filereing exclusion/inclusion for df {config["prep_config_name"]}'s {df_name}"
                )
                logger.info("Cleaning exclusion list for overrides OK")
                df = clean_exclusion_list_with_ovr(
                    df, exclusion_list_col="exclusion_list"
                )
            config["dfs_dict"][df_name] = df

        if config["prep_config_name"] == "portfolio_deltas":
            for df_name, df in config["dfs_dict"].items():
                if df_name == "exclusion_df":
                    # apply clean portfolio and exclusion list
                    # cleant portfolio and exclusion list
                    logger.info("Cleaning portfolio and exclusion lists")
                    df = df.apply(clean_portfolio_and_exclusion_list, axis=1)
                    config["dfs_dict"][df_name] = df

        for df_name, df in config["dfs_dict"].items():
            # remove rows with empyt exclusion lists
            logger.info("Cleaning empty exclusion lists")
            if df_name == "delta_clarity":
                df = clean_empty_exclusion_rows(df, target_col="exclusion_list")
            if df_name == "exclusion_df":
                df = clean_empty_exclusion_rows(df)
            config["dfs_dict"][df_name] = df

        # persist cleaned DataFrames
        for df_name, df in config["dfs_dict"].items():
            final_key = f"{config['prep_config_name']}_{df_name}"
            final_dfs_dict[final_key] = df

    # MINOR DEBUG LOGGING - REMOVE LATER
    logger.info("\n\n==========SHOW dataframes AFTER cleaning==========\n\n")
    for df_name, df in final_dfs_dict.items():

        # logg columns for all the dfs
        logger.info(f"Columns in {df_name}:\n {df.columns.tolist()}\n")
        # Log if df has index and if it does the index name
        if df.index.name:
            logger.info(f"Index name for {df_name}: {df.index.name}")
            logger.info(f"Columns in {df_name}:\n {df.columns.tolist()}\n")
        else:
            logger.info(f"Columns in {df_name}:\n {df.columns.tolist()}\n")

    # Unpack cleaned DataFrames using original names
    delta_clarity = final_dfs_dict["clarity_deltas_delta_clarity"].copy()
    delta_ex_ptf = final_dfs_dict["portfolio_deltas_exclusion_df"].copy()
    delta_in_ptf = final_dfs_dict["portfolio_deltas_inclusion_df"].copy()
    delta_ex_bmk = final_dfs_dict["benchmark_deltas_exclusion_df"].copy()
    delta_in_bmk = final_dfs_dict["benchmark_deltas_inclusion_df"].copy()

    # Free up memory: delete prep structure and final dict
    del prep_config, final_dfs_dict

    # 6. GET STRATEGIES DFS
    if simple:
        logger.info("Getting strategies dfs")

        configurations = [
            {
                "description": "Exclusion BRS Exclusion Analysis at the Portfolio level",
                "var_name": "str_dfs_ex_ptf",
                "input_df": delta_ex_ptf,
                "exclusion_col": "exclusion_list_brs",
                "affected_col": "affected_portfolio_str",
                "brs_source": brs_df_portfolios,
            },
            {
                "description": "Exclusion BRS Exclusion Analysis at the Benchmark level",
                "var_name": "str_dfs_ex_bmk",
                "input_df": delta_ex_bmk,
                "exclusion_col": "exclusion_list_brs",
                "affected_col": "affected_benchmark_str",
                "brs_source": brs_df_benchmarks,
            },
            {
                "description": "Exclusion BRS Inclusion Analysis at the Portfolio level",
                "var_name": "str_dfs_in_ptf",
                "input_df": delta_in_ptf,
                "exclusion_col": "inclusion_list_brs",
                "affected_col": "affected_portfolio_str",
                "brs_source": brs_df_portfolios,
            },
            {
                "description": "Exclusion BRS Inclusion Analysis at the Benchmark level",
                "var_name": "str_dfs_in_bmk",
                "input_df": delta_in_bmk,
                "exclusion_col": "inclusion_list_brs",
                "affected_col": "affected_benchmark_str",
                "brs_source": brs_df_benchmarks,
            },
        ]

        results_str_level_dfs = {}

        for config in configurations:
            logger.info(f"Getting {config['description']}")
            results_str_level_dfs[config["var_name"]] = process_data_by_strategy(
                input_delta_df=config["input_df"],
                strategies_list=delta_test_cols,
                input_df_exclusion_col=config["exclusion_col"],
                df1_lookup_source=df_1,
                df2_lookup_source=df_2,
                brs_lookup_source=config["brs_source"],
                overrides_df=overrides,
                affected_portfolio_col_name=config["affected_col"],
                logger=logger,
            )
    else:
        pass

    # 7.   Get Zombie Analysis
    if zombie:
        logger.info("Getting zombie analysis df")
        zombie_df = zombie_killer(
            clarity_df=df_2_copy,
            brs_carteras=brs_carteras,
            brs_benchmarks=brs_benchmarks,
            crosreference=crosreference,
        )
    else:
        pass

    # 8 SAVE INTO EXCEL

    # create dict of df and df name
    dfs_dict = {
        "excl_carteras": delta_ex_ptf,
        "excl_benchmarks": delta_ex_bmk,
        "excl_clarity": delta_clarity,
        "incl_carteras": delta_in_ptf,
        "incl_benchmarks": delta_in_bmk,
    }
    if zombie:
        dfs_dict["zombie_analysis"] = zombie_df
    else:
        pass

    # save to excel
    if simple:
        for key, df in results_str_level_dfs.items():
            save_excel(df, OUTPUT_DIR, file_name=f"{DATE}{key}")
            logger.info(f"Saved {key} to {OUTPUT_DIR}/{DATE}{key}.xlsx")
        save_excel(dfs_dict, OUTPUT_DIR, file_name=f"{DATE}dfs_pre_ovr_analysis")
        logger.info(f"Saved dfs_dict to {OUTPUT_DIR}/{DATE}dfs_pre_ovr_analysis.xlsx")

    else:
        save_excel(dfs_dict, OUTPUT_DIR, file_name=f"{DATE}dfs_pre_ovr_analysis")
        logger.info(f"Saved dfs_dict to {OUTPUT_DIR}/{DATE}dfs_pre_ovr_analysis.xlsx")


if __name__ == "__main__":
    # check if user wants also the simplified ovr analysis
    args = parse_arguments().parse_args()
    if args.simple:
        # generate simplify over analysis
        logger.info("Generating simplified version of the pre-ovr analysis too")
        main(simple=True)
    else:
        logger.info(
            "No simple flag found! Generating only full version of the pre-ovr analysis"
        )
        main()
