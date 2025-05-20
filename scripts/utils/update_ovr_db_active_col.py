# update_ovr_db_active_col.py

"""
Script to update the OVR database with active columns.
Check latest datafeed and update the OVR database with active columns.
If datafeed value and the override value are the same active column is FALSE.
"""
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

from scripts.utils.config import get_config
from scripts.utils.dataloaders import (
    load_clarity_data,
    load_overrides,
    load_crossreference,
)

# config script
config = get_config("update-ovr-db-active-col", interactive=False, gen_output_dir=False)
logger = config["logger"]
paths = config["paths"]
df_path = paths["CURRENT_DF_WOUTOVR_PATH"]
overrides_path = paths["OVR_PATH"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]

clarity_test_col = [
    "permid",
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

DATE = config["DATE"]

target_cols_overrides = [
    "permid",
    "aladdin_id",
    "clarityid",
    "issuer_name",
    "ovr_target",
    "ovr_value",
    "ovr_active",
    "df_value",
]


def update_df_value_column(
    overrides: pd.DataFrame, df_clarity_filtered: pd.DataFrame
) -> pd.DataFrame:
    """
    Updates the 'df_value' column in the overrides DataFrame using values
    from the df_clarity_filtered DataFrame based on matching 'aladdin_id' and 'ovr_target'.

    Parameters:
    - overrides: DataFrame containing override entries with 'aladdin_id' and 'ovr_target' columns.
    - df_clarity_filtered: Filtered DataFrame from Clarity data with columns including 'aladdin_id'.

    Returns:
    - A new DataFrame with the updated 'df_value' column.
    """
    # Melt df_clarity_filtered to long format for easier matching
    clarity_melted = df_clarity_filtered.melt(
        id_vars="aladdin_id", var_name="ovr_target", value_name="clarity_value"
    )

    # Merge to bring in the matching clarity value
    overrides_updated = overrides.merge(
        clarity_melted, how="left", on=["aladdin_id", "ovr_target"]
    )

    # Update the df_value column only where clarity_value is present
    overrides["df_value"] = overrides_updated["clarity_value"].combine_first(
        overrides["df_value"]
    )

    return overrides


def update_override_active(
    overrides: pd.DataFrame, df_clarity_filtered: pd.DataFrame, logger=None
) -> pd.DataFrame:
    """
    Updates 'ovr_active' in the overrides DataFrame by comparing 'ovr_value'
    with corresponding values from df_clarity_filtered.

    Parameters:
    - overrides: DataFrame with 'permid', 'ovr_target', 'ovr_value'.
    - df_clarity_filtered: DataFrame with clarity data, structured horizontally.
    - logger: Logger object (optional).

    Returns:
    - overrides: DataFrame with updated 'ovr_active' status.
    """

    # Melt df_clarity_filtered to match overrides structure
    clarity_melted = df_clarity_filtered.melt(
        id_vars="permid", var_name="ovr_target", value_name="clarity_value"
    )

    # Merge with overrides to get clarity_value aligned
    overrides_merged = overrides.merge(
        clarity_melted, on=["permid", "ovr_target"], how="left"
    )

    # Set ovr_active to False if ovr_value matches clarity_value
    condition = overrides_merged["ovr_value"] == overrides_merged["clarity_value"]

    # Set ovr_active to False where condition is True
    overrides.loc[condition, "ovr_active"] = False

    # Optional logging
    if logger:
        inactive_rows = overrides_merged[condition]
        for _, row in inactive_rows.iterrows():
            logger.info(
                f"Override on {row['ovr_target']} for permid {row['permid']} is not active"
            )

    return overrides


def main():
    # load data

    df_clarity = load_clarity_data(df_path, target_cols=clarity_test_col)
    overrides = load_overrides(
        overrides_path, target_cols=target_cols_overrides, drop_active=False
    )
    # save back columns for backup
    overrides_copy = overrides.copy()
    if "brs_id" in overrides.columns:
        overrides.rename(columns={"brs_id": "aladdin_id"}, inplace=True)
    crossreference = load_crossreference(CROSSREFERENCE_PATH)
    logger.info(f"override columns are {overrides.columns}")
    logger.info
    # add aladdin_id to df_clarity from crossreference
    df_clarity = df_clarity.merge(
        crossreference[["permid", "aladdin_id"]], on="permid", how="left"
    )

    # set permid in df_clarity & overrides as str
    df_clarity["aladdin_id"] = df_clarity["aladdin_id"].astype(str)
    overrides["aladdin_id"] = overrides["aladdin_id"].astype(str)

    # filter out only the columns we need with using the relevant permids
    df_clarity_filtered = df_clarity[
        df_clarity["aladdin_id"].isin(overrides["aladdin_id"])
    ].copy()

    if df_clarity_filtered.shape[0] < 1:
        logger.warning("No matches between df_clarity and overrrides!")
        sys.exit()

    logger.info(f"Size df_clarity_filterd is {df_clarity_filtered.shape[0]}")

    # define output paths
    base_path = Path(
        r"C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides"
    )
    current_date = datetime.now().strftime("%Y%m%d")
    output_file = base_path / "overrides_db_beta.xlsx"
    backup_file = base_path / "overrides_db_backup" / f"{DATE}_override_db.xlsx"
    deactivated_overrides_file = (
        base_path / f"{current_date}_{DATE}_deactivated_overrides.xlsx"
    )

    # update active status of overrides
    logger.info("updating overrides active status")
    overrides = update_override_active(overrides, df_clarity_filtered, logger)

    # update active column df_value of overrides with data from df_clarity
    logger.info("Updating overrides df_value column")
    overrides = update_df_value_column(overrides, df_clarity_filtered)

    # RETURN DF OF OVERRIDES THAT HAS BEEN DEACTIVATED
    deactivated_overrides = overrides[overrides["ovr_active"] == False]
    # log length of deactivated overrides
    logger.info(f"Number of deactivated overrides: {len(deactivated_overrides)}")

    # save the updated overrides to the output file
    logger.info(
        f"Saving updated overrides to {output_file}\nand backup to {backup_file}"
    )
    overrides.to_excel(output_file, index=False)
    overrides_copy.to_excel(backup_file, index=False)
    deactivated_overrides.to_excel(deactivated_overrides_file, index=False)


if __name__ == "__main__":
    main()
    logger.info("Script completed successfully.")
