import warnings
from datetime import datetime
from pathlib import Path
import os
from dateutil.relativedelta import relativedelta


import pandas as pd
from utils.set_up_log import set_up_log
from utils.get_date import get_date
from utils.dataloaders import load_clarity_data, load_aladdin_data, load_crossreference

# Set up logging
logger = set_up_log("zombie-killer")
# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# DEFINE  CONSTANTS
# Get user input for date
DATE = get_date()
YEAR = DATE[:4]
date_obj = datetime.strptime(DATE, "%Y%m")
prev_date_obj = date_obj - relativedelta(months=1)
DATE_PREV = prev_date_obj.strftime("%Y%m")

# DEFINE TEST & MERGING COLUMNS
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

merging_cols = [
    "aladdin_id",
    "str_001_s",
    "str_002_ec",
    "str_003b_ec",
    "str_003_ec",
    "str_004_asec",
    "str_005_ec",
    "str_006_sec",
    "str_007_sect",
    "gp_esccp_22",
    "gp_esccp_25",
    "gp_esccp_30",
]

# DEFINE PATHS
REPO_DIR = Path(r"C:\Users\n740789\Documents\clarity_data_quality_controls")
DATAFEED_DIR = Path(r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED")
clarity_df_path = (
    DATAFEED_DIR
    / "ficheros_tratados"
    / f"{YEAR}"
    / f"{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"
)
ALADDIN_DATA_DIR = REPO_DIR / "excel_books" / "aladdin_data"
CROSSREFERENCE_PATH = (
    ALADDIN_DATA_DIR / "crossreference" / f"Aladdin_Clarity_Issuers_{DATE}01.csv"
)
BMK_PORTF_STR_PATH = (
    ALADDIN_DATA_DIR / "bmk_portf_str" / f"{DATE}_strategies_snt world_portf_bmks.xlsx"
)
SRI_DATA_DIR = REPO_DIR / "excel_books" / "sri_data"
OVR_PATH = (
    REPO_DIR / "excel_books" / "sri_data" / "overrides" / "20250318_overrides_db.xlsx"
)

OUTPUT_DIR = SRI_DATA_DIR / "zombie_list"


#  DEFINE FUNCTIONS
def mark_zombies(df, merging_cols):

    def get_zombie_columns(row):
        # List to hold the base names where _brs has a value but _df is NaN.
        zombie_cols = []
        for col in merging_cols:
            col_brs = f"{col}_brs"
            col_df = f"{col}_df"
            # Check that both columns exist in the row (they should if the merge worked as expected)
            if col_brs in row and col_df in row:
                # if _brs is not NaN and _df is NaN, add the column name (without suffix)
                if pd.notna(row[col_brs]) and pd.isna(row[col_df]):
                    zombie_cols.append(col)
        return zombie_cols

    # Create the zombie_list column by applying the function to each row
    df["zombie_list"] = df.apply(get_zombie_columns, axis=1)
    # Create the zombie_flag column: True if the zombie_list is not empty
    df["zombie_flag"] = df["zombie_list"].apply(lambda lst: len(lst) > 0)

    return df


def column_sorter(df):
    # order columns
    columns_order = [
        # General identifiers
        "issuer_name",
        "aladdin_id",
        "security_description",
        "portfolio_full_name",
        "portfolio_id",
        # str scores (BRS then DF)
        "str_001_s_brs",
        "str_001_s_df",
        "str_002_ec_brs",
        "str_002_ec_df",
        "str_003b_ec_brs",
        "str_003b_ec_df",
        "str_003_ec_brs",
        "str_003_ec_df",
        "str_004_asec_brs",
        "str_004_asec_df",
        "str_004_asec_sust._bonds",
        "str_005_ec_brs",
        "str_005_ec_df",
        "str_006_sec_brs",
        "str_006_sec_df",
        "str_007_sect_brs",
        "str_007_sect_df",
        "str_008_sec",
        "str_009_tec",
        # gp scores
        "gp_esccp_22_brs",
        "gp_esccp_22_df",
        "gp_esccp_25_brs",
        "gp_esccp_25_df",
        "gp_esccp_30_brs",
        "gp_esccp_30_df",
        "gp_essccp",
        # scs scores
        "scs_001_sec",
        "scs_002_ec",
        "scs_003_sec",
        # zombie
        "zombie_flag",
        "zombie_list",
    ]

    return df[columns_order]


def group_by_security_description(df):
    """
    Group by 'security_description' and create:
      - 'portfolio_list': list of all unique portfolio_full_names per security
      - 'portfolio_id_list': list of all unique portfolio_ids per security
      - Keep 'issuer_name' (first occurrence) and 'aladdin_id' (first occurrence)
      - Combine all 'zombie_list' entries (which are lists) into one list and rename it as 'strategy_list'
    """

    def flatten_lists(series):
        """
        Given a pandas Series where each element is a list, flatten them into a single list
        and return unique values while preserving their order.
        """
        flattened = []
        for item in series:
            # Check if the item is a list; if not, treat it as a single element.
            if isinstance(item, list):
                flattened.extend(item)
            else:
                flattened.append(item)
        # Remove duplicates while preserving order
        seen = set()
        unique_items = []
        for i in flattened:
            if i not in seen:
                seen.add(i)
                unique_items.append(i)
        return unique_items

    agg_dict = {
        # Assuming issuer_name and aladdin_id are the same for a given security_description
        "issuer_name": "first",
        "aladdin_id": "first",
        # Collect unique portfolio names and ids
        "portfolio_full_name": lambda x: list(x.unique()),
        "portfolio_id": lambda x: list(x.unique()),
        # For zombie_list, use the custom flatten function
        "zombie_list": lambda x: flatten_lists(x),
    }

    # Group the DataFrame
    grouped_df = df.groupby("security_description", as_index=False).agg(agg_dict)

    # Rename columns accordingly
    grouped_df.rename(
        columns={
            "portfolio_full_name": "portfolio_list",
            "portfolio_id": "portfolio_id_list",
            "zombie_list": "strategy_list",
        },
        inplace=True,
    )

    # Reorder columns: issuer_name, aladdin_id, security_description, strategy_list, portfolio_list, portfolio_id_list
    final_columns = [
        "issuer_name",
        "aladdin_id",
        "security_description",
        "strategy_list",
        "portfolio_list",
        "portfolio_id_list",
    ]
    grouped_df = grouped_df[final_columns]

    return grouped_df


def main():
    # 00 LOAD DATA
    columns_to_read = ["permid", "isin", "issuer_name"] + test_col
    # clarity data
    clarity_df = load_clarity_data(clarity_df_path, columns_to_read)
    # aladdin-brs data
    brs_carteras = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_carteras")
    brs_benchmarks = load_aladdin_data(BMK_PORTF_STR_PATH, "portfolio_benchmarks")
    crosreference = load_crossreference(CROSSREFERENCE_PATH)

    # 01 PROCESS DATA
    # add aladdin_id from crossreference to clarity_df
    clarity_df = clarity_df.merge(
        crosreference[["aladdin_id", "permid"]], on="permid", how="left"
    )

    # remove rows with no aladdin_id
    brs_carf = brs_carteras[~(brs_carteras.aladdin_id.isna())].copy()
    # merged data from aladdin with clarity
    merged_df = brs_carf.merge(
        clarity_df[merging_cols], on="aladdin_id", how="left", suffixes=("_brs", "_df")
    )

    # find zombies
    zombie_df = mark_zombies(merged_df, merging_cols)
    # sort columns
    zombie_df = column_sorter(zombie_df)
    # keep only rows with zombie_flag TRUE
    zombie_df = zombie_df[zombie_df.zombie_flag]
    # groubby security to summarise information
    zombie_grouped = group_by_security_description(zombie_df)

    return zombie_grouped


# if __name__ == "__main__": save zombie_grouped to csv
if __name__ == "__main__":
    OUTPUT_FILE = OUTPUT_DIR / f"{DATE}_zombie_analysis.csv"
    zombie_grouped = main()
    zombie_grouped.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Zombie analysis saved to {OUTPUT_FILE}")
    print(f"Zombie analysis saved to {OUTPUT_FILE}")
