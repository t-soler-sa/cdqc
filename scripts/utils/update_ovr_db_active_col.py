# update_ovr_db_active_col.py

"""
Script to update the OVR database with active columns.
Check latest datafeed and update the OVR database with active columns.
If datafeed value and the override value are the same active column is FALSE.
"""
from datetime import datetime
from pathlib import Path

import pandas as pd

from scripts.utils.config import get_config
from scripts.utils.dataloaders import load_clarity_data

config = get_config("update_ovr_db_active_col", interactive=False, gen_output_dir=False)
logger = config["logger"]

clarity_test_col = [
    "permid",
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

DATE = datetime.now().strftime("%Y%m%d")


def load_overrides(path):
    try:
        return pd.read_excel(path)
    except Exception as e:
        logger.error(f"Failed to load overrides file: {e}")
        raise


def override_active(row, df_clarity):
    permid = row["permid"]
    ovr_value = row["ovr_value"]
    ovr_target = row["ovr_target"]

    if (df_clarity["permid"] == permid).any() and (ovr_target in df_clarity.columns):
        df_val = df_clarity.loc[df_clarity["permid"] == permid, ovr_target].values[0]
        is_active = df_val != ovr_value
        if not is_active:
            logger.info(f"Override on {ovr_target} for permid {permid} is not active")
        return is_active
    else:
        return None


def main():
    # config & load data
    df_path = config["paths"]["CURRENT_DF_WOUTOVR_PATH"]
    df_clarity = load_clarity_data(df_path, target_cols=clarity_test_col)
    overrides_path = config["paths"]["OVR_PATH"]
    overrides = load_overrides(overrides_path)

    # save back columns for backup
    overrides_copy = overrides.copy()

    # filter out only the columns we need with using the relevant permids
    df_clarity_filtered = df_clarity[
        df_clarity["permid"].isin(overrides["permid"])
    ].copy()

    # define output paths
    output_file = Path(
        r"C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides\overrides_db_beta.xlsx"
    )

    backup_file = Path(
        rf"C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides\overrides_db_backup\{DATE}_override_db.xlsx"
    )

    # update active status of overrides
    logger.info(f"updating overrides active status")
    for idx, row in overrides.iterrows():
        ovr_active = override_active(row, df_clarity_filtered)
        if ovr_active is False:
            overrides.at[idx, "ovr_active"] = False

    overrides = overrides[overrides_copy.columns]

    logger.info(
        f"Saving updated overrides to {output_file}\nand backup to {backup_file}"
    )
    overrides.to_excel(output_file, index=False)
    overrides_copy.to_excel(backup_file, index=False)


if __name__ == "__main__":
    main()
    logger.info("Script completed successfully.")
