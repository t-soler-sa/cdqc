import logging
from typing import List, Tuple

import numpy as np
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_data(file_path: str, columns: List[str]) -> pd.DataFrame:
    """Load data from CSV file."""
    return pd.read_csv(file_path, sep=",", dtype="unicode", usecols=columns)


def prepare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare DataFrames by setting index and filtering for common indexes."""
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

    DATE_PREV = "202409"
    DATE = "202410"

    df_1_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE_PREV}01_Equities_feed_IssuerLevel_sinOVR.csv"
    df_2_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"

    columns_to_read = ["permid", "isin", "issuer_name"] + test_col

    df_1 = load_data(df_1_path, columns_to_read)
    df_2 = load_data(df_2_path, columns_to_read)

    logging.info(f"df_1 shape: {df_1.shape}, df_2 shape: {df_2.shape}")

    df_1, df_2 = prepare_dataframes(df_1, df_2)

    delta = compare_dataframes(df_1, df_2, test_col)

    delta = check_new_exclusions(df_1, df_2, delta, test_col)
    delta = check_new_inclusions(df_1, df_2, delta, test_col)

    delta = finalize_delta(delta, test_col)

    output_file = "delta_results.csv"
    delta.to_csv(output_file, index=False)
    logging.info(f"Results saved to {output_file}")

    logging.info("Analysis completed successfully.")


if __name__ == "__main__":
    main()
