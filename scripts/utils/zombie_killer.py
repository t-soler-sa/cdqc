import sys
import warnings
from pathlib import Path

import pandas as pd

# Ensure the parent directory (which contains config.py) is in sys.path.
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Import the centralized configuration
from utils.config import get_config
from utils.dataloaders import load_aladdin_data, load_clarity_data, load_crossreference

# Get the common configuration for the zombie-killer script.
config = get_config("zombie-killer", interactive=False)
logger = config["logger"]
DATE = config["DATE"]
YEAR = config["YEAR"]
DATE_PREV = config["DATE_PREV"]

# Retrieve paths from config
paths = config["paths"]
clarity_df_path = paths["CURRENT_DF_WOUTOVR_PATH"]
BMK_PORTF_STR_PATH = paths["BMK_PORTF_STR_PATH"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]

SRI_DATA_DIR = config["SRI_DATA_DIR"]
OUTPUT_DIR = config["OUTPUT_DIR"]

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Define test, merging, and columns to read
test_col = [
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

merging_cols = [
    "aladdin_id",
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

columns_to_read = ["permid", "isin", "issuer_name"] + test_col

rename_dict = {
    "cs_001_sec": "scs_001_sec",
    "cs_002_ec": "scs_002_ec",
    "art_8_basicos": "str_sfdr8_aec",
}


# Define functions
def mark_zombies(df, merging_cols):
    def get_zombie_columns(row):
        # List to hold the base names where _brs has a value but _df is NaN.
        zombie_cols = []
        for col in merging_cols:
            col_brs = f"{col}_brs"
            col_df = f"{col}_df"
            # Check that both columns exist in the row.
            if col_brs in row and col_df in row:
                # If _brs is not NaN and _df is NaN, add the column name.
                if pd.notna(row[col_brs]) and pd.isna(row[col_df]):
                    zombie_cols.append(col)
        return zombie_cols

    # Create the zombie_list and zombie_flag columns.
    df["zombie_list"] = df.apply(get_zombie_columns, axis=1)
    df["zombie_flag"] = df["zombie_list"].apply(lambda lst: len(lst) > 0)
    return df


def column_sorter(df):
    # Order columns as desired.
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
        "str_sfdr8_aec_brs",
        "str_sfdr8_aec_df",
        "scs_001_sec_brs",
        "scs_001_sec_df",
        "scs_002_ec_brs",
        "scs_002_ec_df",
        # zombie
        "zombie_flag",
        "zombie_list",
    ]
    return df[columns_order]


def group_by_security_description(df):
    """
    Group by 'security_description' and create:
      - 'portfolio_list': list of all unique portfolio_full_names per security.
      - 'portfolio_id_list': list of all unique portfolio_ids per security.
      - Keep 'issuer_name' (first occurrence) and 'aladdin_id' (first occurrence).
      - Combine all 'zombie_list' entries into one list named 'strategy_list'.
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
    grouped_df.rename(
        columns={
            "portfolio_full_name": "portfolio_list",
            "portfolio_id": "portfolio_id_list",
            "zombie_list": "strategy_list",
        },
        inplace=True,
    )

    # Reorder columns
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
    clarity_df = load_clarity_data(clarity_df_path, columns_to_read)
    clarity_df.rename(columns=rename_dict, inplace=True)
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
        clarity_df[merging_cols],
        on="aladdin_id",
        how="left",
        suffixes=("_brs", "_df"),
    )

    # now the same for benchmarks
    brs_bench = brs_benchmarks[~(brs_benchmarks.aladdin_id.isna())].copy()

    # find zombies
    zombie_df = mark_zombies(merged_df, merging_cols)
    # sort columns
    zombie_df = column_sorter(zombie_df)
    # keep only rows with zombie_flag TRUE
    zombie_df = zombie_df[zombie_df.zombie_flag]
    # groubby security to summarise information
    zombie_grouped = group_by_security_description(zombie_df)

    return zombie_grouped


# When the script is run, save the output to a CSV file.
if __name__ == "__main__":
    OUTPUT_FILE = OUTPUT_DIR / f"{DATE}_zombie_analysis.csv"
    zombie_grouped = main()
    zombie_grouped.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Zombie analysis saved to {OUTPUT_FILE}")
    print(f"Zombie analysis saved to {OUTPUT_FILE}")
