# update_ovr_db_active_col

"""
Script to update the OVR database with active columns.
Check latest datafeed and update the OVR database with active columns.
If datafeed value and the override value are the same active column is FALSE.

"""
from pathlib import Path

from utils.dataloaders import load_clarity_data, load_overrides
from utils.config import get_config


# config
config = get_config("update_ovr_db_active_col", interactive=True)
logger = config["logger"]

# constants
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

# Load the latest datafeed & override db
df_path = config["paths"]["NEW_DF_WOVR_PATH"]
df_clarirty = load_clarity_data(df_path, target_cols=clarity_test_col)
overrides_path = config["paths"]["OVR_DB_PATH"]
overrides = load_overrides(overrides_path)

# filter df_clarirty
df_clarirty = df_clarirty[df_clarirty["permid"].isin(overrides["permid"])].copy()


# define override_active function
def override_active(row):
    permid = row["permid"]
    ovr_value = row["ovr_value"]
    ovr_target = row["ovr_target"]

    if (df_clarirty["permid"] == permid).any() and (ovr_target in df_clarirty.columns):
        df_val = df_clarirty.loc[df_clarirty["permid"] == permid, ovr_target].values[0]
        is_active = df_val != ovr_value
        if not is_active:
            logger.info(f"Override on {ovr_target} for permid {permid} is not active")
        return is_active
    else:
        logger.warning(f"Permid {permid} or column {ovr_target} not found in datafeed")
        return None


def main():
    logger.info("Override Active Update Script started")
    output_file = Path(
        r"C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides\overrides_db_beta.xlsx"
    )

    for idx, row in overrides.iterrows():
        logger.info(f"Updating row {idx}")
        ovr_active = override_active(row)
        if ovr_active is not None:
            overrides.at[idx, "ovr_active"] = ovr_active

    logger.info(f"Saving updated overrides to {output_file}")
    overrides.to_excel(output_file, index=False)
