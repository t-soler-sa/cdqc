# non-compliance_analysis.py

import warnings

from scripts.utils.dataloaders import (
    load_clarity_data,
    load_aladdin_data,
    load_crossreference,
    load_portfolios,
    save_excel,
)

# Import the centralized configuration
from scripts.utils.config import get_config

# import relevant libraries from 00_preovr_analysis
from scripts.utils.clarity_data_quality_control_functions import (
    prepare_dataframes,
    generate_delta,
    add_portfolio_benchmark_info_to_df,
    get_issuer_level_df,
    filter_empty_lists,
    filter_rows_with_common_elements,
    reorder_columns,
    clean_portfolio_and_exclusion_list,
)


# Get the common configuration for the Pre-OVR-Analysis script.
config = get_config(
    "noncomplieance-analysis",
    interactive=True,
    gen_output_dir=True,
    output_dir_dated=True,
    auto_date=True,
)
logger = config["logger"]
DATE = config["DATE"]
REPO_DIR = config["REPO_DIR"]
DATAFEED_DIR = config["DATAFEED_DIR"]
SRI_DATA_DIR = config["SRI_DATA_DIR"]
paths = config["paths"]

# Use the paths from config
df_path = paths["NEW_DF_WOVR_PATH"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]
BMK_PORTF_STR_PATH = paths["BMK_PORTF_STR_PATH"]
COMMITTEE_PATH = paths["COMMITTEE_PATH"]

# Define the output directory and file based on the configuration.
OUTPUT_DIR = config["OUTPUT_DIR"]
OUTPUT_FILE = OUTPUT_DIR / f"{DATE}_noncompliance_analysis.xlsx"

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# DEFINE CONTANTS

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
brs_test_cols = ["aladdin_id"] + delta_test_cols

rename_dict = {
    "cs_001_sec": "scs_001_sec",
    "cs_002_ec": "scs_002_ec",
    "art_8_basicos": "str_sfdr8_aec",
}


# DEFINE MAIN FUNCTION
def main():
    logger.info(f"Starting pre-ovr-analysis for {DATE}.")
    # 1.    LOAD DATA

    # 1.1.  aladdin /brs data / perimeters
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    crossreference = load_crossreference(CROSSREFERENCE_PATH)
    # remove duplicate and nan permid in crossreference
    logger.info("Removing duplicates and NaN values from crossreference")
    crossreference.drop_duplicates(subset=["permid"], inplace=True)
    crossreference.dropna(subset=["permid"], inplace=True)
    # get BRS data at issuer level for becnhmarks without empty aladdin_id
    brs_carteras_issuerlevel = get_issuer_level_df(brs_carteras, "aladdin_id")

    # 1.2.  clarity data
    df = load_clarity_data(df_path, columns_to_read)
    # let's rename columns in df_1 and df using the rename_dict
    df.rename(columns=rename_dict, inplace=True)
    # add aladdin_id to df_1 and df
    logger.info("Adding aladdin_id to clarity dfs")
    df = df.merge(crossreference[["permid", "aladdin_id"]], on="permid", how="left")

    # Load portfolios & benchmarks dicts
    (
        portfolio_dict,
        benchmark_dict,
    ) = load_portfolios(path_pb=BMK_PORTF_STR_PATH, path_committe=COMMITTEE_PATH)

    # 2.    PREP DATA FOR ANALYSIS
    # make sure that the values of of the columns delta_test_cols are strings and all uppercase and strip
    for col in delta_test_cols:
        df[col] = df[col].str.upper().str.strip()
        brs_carteras_issuerlevel[col] = (
            brs_carteras_issuerlevel[col].str.upper().str.strip()
        )

    # 2.3.  PREPARE DATA BRS LEVEL FOR PORTFOLIOS
    (
        brs_df,
        clarity_df,
        in_clarity_but_not_in_brs,
        in_brs_but_not_in_clarity,
    ) = prepare_dataframes(brs_carteras_issuerlevel, df, target_index="aladdin_id")

    # log size of new and missing issuers
    logger.info(
        f"Number issuers in clarity but not Aladdin: {in_clarity_but_not_in_brs.shape[0]}"
    )
    logger.info(
        f"Number issuers in Aladdin but not Clarity: {in_brs_but_not_in_clarity.shape[0]}"
    )

    # START NONCOMPLIANCE ANALYSIS
    # COMPARE DATA
    logger.info("checking impact compared to BRS portfolio data")
    delta_brs = generate_delta(
        brs_df,
        clarity_df,
        target_index="aladdin_id",
        delta_analysis_str="exclusion",
        condition_list=["EXCLUDED"],
        delta_name_str="delta_brs_ptf",
        filter_col="new_exclusion",
        drop_cols=["new_inclusion", "inclusion_list"],
    )

    # 4.    PREP DELTAS BEFORE SAVING
    # PREP DELTAS BEFORE SAVING
    logger.info("Preparing deltas before saving")
    # drop isin from deltas
    delta_brs.drop(columns=["isin"], inplace=True)

    # let's add portfolio info to the delta_df
    delta_brs = add_portfolio_benchmark_info_to_df(portfolio_dict, delta_brs)

    # let's add benchmark info to the delta_df
    delta_brs = add_portfolio_benchmark_info_to_df(
        benchmark_dict, delta_brs, "affected_benchmark_str"
    )

    # 5. FILTER & SORT DATA & GET RELEVANT DATA FOR THE ANALYSIS
    # let's use filter_non_empty_lists to remove rows with empty lists in affected_portfolio_str
    delta_brs = filter_empty_lists(delta_brs, "affected_portfolio_str")

    # pass filter_rows_with_common_elements for columns exclusion_list and affected_portfolio_str
    delta_brs = filter_rows_with_common_elements(
        delta_brs, "exclusion_list", "affected_portfolio_str"
    )

    # 7. SAVE TO EXCEL

    # 7.1. sort columns of deltas df before saving
    # set id_name_issuers_cols first and exclude delta_test_cols
    delta_brs = reorder_columns(delta_brs, id_name_issuers_cols, delta_test_cols)

    # remove columns "new_exclusion", "new_inclusion", "inclusion_list", and "affected_benchmark_str" from delta_brs
    target_drop_columns = [
        "new_exclusion",
        "new_inclusion",
        "inclusion_list",
        "affected_benchmark_str",
    ]
    columns_to_drop = [col for col in target_drop_columns if col in delta_brs.columns]
    logger.info(f"Dropping columns: {columns_to_drop}")
    delta_brs.drop(
        columns=columns_to_drop,
        inplace=True,
    )

    # cleant portfolio and exclusion list
    delta_brs = delta_brs.apply(clean_portfolio_and_exclusion_list, axis=1)

    # create dict of df and df name
    dfs_dict = {
        "incumplimientos": delta_brs,
    }

    # save to excel
    save_excel(dfs_dict, OUTPUT_DIR, file_name="noncomplience_analysis")


if __name__ == "__main__":
    main()
