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
import numpy as np
from pandas.api.types import is_scalar

from scripts.utils.config import get_config
from scripts.utils.dataloaders import (
    load_clarity_data,
    load_overrides,
    load_crossreference,
)
from scripts.utils.clarity_data_quality_control_functions import (
    log_df_head_compact,
    pad_identifiers,
)

# config script
config = get_config("update-ovr-db-active-col", interactive=False, gen_output_dir=False)
logger = config["logger"]
DATE = config["DATE"]
paths = config["paths"]
df_path = paths["CURRENT_DF_WOUTOVR_PATH"]
overrides_path = paths["OVR_PATH"]
overrides_dir_path = overrides_path.parent
crossreference_path = paths["CROSSREFERENCE_PATH"]
brs_issuer_data_dir = config["BRS_ISSUER_DATA_DIR_PATH"]
DATE_STAMP = datetime.now().strftime("%Y%m%d")
brs_issuer_data_file: Path = brs_issuer_data_dir / f"{DATE_STAMP}_brs_issuer_data.csv"

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


# Define Regular functions
def update_df_value_column(
    overrides: pd.DataFrame,
    df_clarity_filtered: pd.DataFrame,
) -> pd.DataFrame:
    """
    Replace overrides['df_value'] with the value that is currently in
    the (deduplicated) Clarity feed, matched on aladdin_id + ovr_target.
    """

    # ── 1.  Melt feed, keep only non-null values ────────────────────────────────
    clarity_long = df_clarity_filtered.melt(
        id_vars="aladdin_id", var_name="ovr_target", value_name="clarity_value"
    ).dropna(subset=["clarity_value"])

    # ── 2.  Detect duplicates BEFORE the merge  ────────────────────────────────
    dup_mask = clarity_long.duplicated(["aladdin_id", "ovr_target"], keep=False)
    if dup_mask.any():
        raise ValueError(
            "[DQ] duplicate aladdin_id / ovr_target pairs in clarity feed:\n"
            f"{clarity_long.loc[dup_mask].head()}"
        )

    # ── 3.  Merge and update, but preserve the original index ─────────────────
    clarity_map = clarity_long.set_index(["aladdin_id", "ovr_target"])["clarity_value"]

    # vectorised lookup without changing the row order or index
    overrides_idx = list(zip(overrides["aladdin_id"], overrides["ovr_target"]))
    new_vals = clarity_map.reindex(overrides_idx).values

    overrides["df_value"] = np.where(
        pd.notna(new_vals), new_vals, overrides["df_value"]
    )

    return overrides


def update_override_active(
    overrides: pd.DataFrame,
    df_clarity_filtered: pd.DataFrame,
) -> pd.DataFrame:

    clarity_melted = df_clarity_filtered.melt(
        id_vars="aladdin_id", var_name="ovr_target", value_name="clarity_value"
    ).dropna(subset=["clarity_value"])

    duplicates = clarity_melted.duplicated(["aladdin_id", "ovr_target"])
    if duplicates.any():
        dup_keys = clarity_melted.loc[duplicates, ["aladdin_id", "ovr_target"]]
        raise ValueError(
            f"[DQ] {len(dup_keys)} duplicate keys in clarity feed:\n"
            f"{dup_keys.head().to_string(index=False)}"
        )

    overrides_merged = overrides.merge(
        clarity_melted, on=["aladdin_id", "ovr_target"], how="left"
    )

    condition = overrides_merged["ovr_value"] == overrides_merged["df_value"]

    try:
        overrides.loc[condition.values, "ovr_active"] = False
    except Exception as e:
        logger.error("Error updating overrides: %s", e)
        raise

    return overrides


def find_conflicting_columns(
    df: pd.DataFrame,
    id_col: str = "aladdin_id",
    conflict_col_a: str = "ovr_target",
    conflict_col_b: str = "ovr_value",
) -> pd.DataFrame:
    grouping_cols = [id_col, conflict_col_a]
    target_cols = grouping_cols + [conflict_col_b]

    # Step 1: Count unique conflict_col_b values per group
    grouped_df = df.groupby(grouping_cols)[conflict_col_b].nunique()

    # Step 2: Filter to groups with more than one unique conflict_col_b
    conflicting_keys = grouped_df[grouped_df > 1].index

    # Step 3: Use a mask to filter original DataFrame
    mask = df.set_index(grouping_cols).index.isin(conflicting_keys)
    return df[mask].sort_values(by=grouping_cols)[target_cols].copy()


def main():

    # load brs issuer data in ptf and bkm
    logger.info(f"Loading BRS issuer data from {brs_issuer_data_file}")
    if not brs_issuer_data_file.exists():
        logger.error(f"BRS issuer data file {brs_issuer_data_file} does not exist.")
        sys.exit(1)

    brs_issuer_data = pd.read_csv(
        brs_issuer_data_file,
        usecols=["issuer_id", "ultimate_issuer_id"],
        dtype="str",
        low_memory=False,
    )

    brs_issuer_data.rename(columns={"issuer_id": "aladdin_id"}, inplace=True)
    brs_issuer_data["aladdin_id"] = brs_issuer_data["aladdin_id"].apply(
        lambda x: str(x) if pd.notna(x) else x
    )
    brs_issuer_data["aladdin_id"] = pad_identifiers(brs_issuer_data["aladdin_id"])
    brs_issuer_data.drop_duplicates(subset=["aladdin_id"], inplace=True)

    # load clarity data
    df_clarity = load_clarity_data(df_path, target_cols=clarity_test_col)
    df_clarity.loc[:, "permid"] = df_clarity.permid.astype(
        str
    )  # ensure permid is str type

    # log_df_head_compact(df_clarity, df_name="df_clarity")
    overrides = load_overrides(
        overrides_path, target_cols=target_cols_overrides, drop_active=False
    )
    overrides["aladdin_id"] = pad_identifiers(overrides["aladdin_id"])
    # log_df_head_compact(overrides, df_name="overrides")
    troubles_overrides = find_conflicting_columns(overrides)
    # log_df_head_compact(troubles_overrides, df_name="troubles_overrides")

    logger.info(
        f"\ntroubles_overrides first 10 rows is {troubles_overrides.head(10)}\n"
    )

    logger.info(f"There are {len(troubles_overrides)} conflicting rows in overrides\n")

    crossreference = load_crossreference(crossreference_path)
    # log_df_head_compact(crossreference, df_name="crossreference_raw")
    logger.info("Removing duplicates and NaN values from crossreference")
    crossreference = crossreference.dropna(subset=["permid"]).drop_duplicates(
        subset=["permid"]
    )
    # log_df_head_compact(crossreference, df_name="crossreference_cleaned")
    # set permid in crossreference as str
    crossreference["permid"] = crossreference["permid"].apply(
        lambda x: str(x) if pd.notna(x) else x
    )

    # add aladdin_id to df_clarity from crossreference
    df_clarity = df_clarity.merge(
        crossreference[["permid", "aladdin_id"]], on="permid", how="left"
    )
    log_df_head_compact(df_clarity, df_name="df_clarity_with_aladdin_id")
    # set permid in df_clarity & overrides as str
    logger.info("Setting permid in df_clarity & overrides as str - to avoid issues")
    df_clarity["aladdin_id"] = df_clarity["aladdin_id"].apply(
        lambda x: str(x) if pd.notna(x) else x
    )
    overrides["aladdin_id"] = overrides["aladdin_id"].astype(str)

    empty_aladdin_rows = df_clarity["aladdin_id"].isna().sum()
    duplicated_aladdin_rows = df_clarity["aladdin_id"].duplicated().sum()
    logger.info(
        f"""\nRows with empty aladdin_id on df_clarity: {empty_aladdin_rows}.
        \nRows with duplicate {duplicated_aladdin_rows}."""
    )
    if empty_aladdin_rows > 0:
        logger.info(f"We will drop Rows with empty aladdin_id on df_clarity.")
        # drop rows with empty aladdin_id on df_clarity
        df_clarity = df_clarity.dropna(subset=["aladdin_id"])

    if duplicated_aladdin_rows > 0:
        logger.info(f"We will drop Rows with duplicate aladdin_id on df_clarity.")
        # drop rows with duplicate aladdin_id on df_clarity
        df_clarity = df_clarity.drop_duplicates(subset=["aladdin_id"])

    # filter out only the columns we need with using the relevant permids
    df_clarity_filtered = df_clarity[
        df_clarity["aladdin_id"].isin(overrides["aladdin_id"])
    ].copy()

    if df_clarity_filtered.shape[0] < 1:
        logger.warning("No matches between df_clarity and overrrides!")
        sys.exit()

    logger.info(f"Size df_clarity_filterd is {df_clarity_filtered.shape[0]}")

    # define output paths
    current_date = datetime.now().strftime("%Y%m%d")
    output_file = overrides_dir_path / f"{current_date}_{DATE}_overrides_db_beta.xlsx"

    # update active column df_value of overrides with data from df_clarity
    logger.info("Updating overrides df_value column")
    overrides = update_df_value_column(overrides, df_clarity_filtered)

    # update active status of overrides
    logger.info("updating overrides active status")
    overrides = update_override_active(overrides, df_clarity_filtered)
    log_df_head_compact(overrides, df_name="overrides_updated")

    # RETURN DF OF OVERRIDES THAT HAS BEEN DEACTIVATED
    deactivated_overrides = overrides[overrides["ovr_active"] == False]
    # log_df_head_compact(deactivated_overrides, df_name="deactivated_overrides")
    # log length of deactivated overrides
    logger.info(f"Number of deactivated overrides: {len(deactivated_overrides)}")

    # add ultimate_issuer_id from brs_issuer_data to overrides
    logger.info("Adding ultimate_issuer_id from brs_issuer_data to overrides")
    overrides = overrides.merge(
        brs_issuer_data[["aladdin_id", "ultimate_issuer_id"]],
        on="aladdin_id",
        how="left",
    )

    # place column ultimate_issuer_id behind aladdin_id
    cols = overrides.columns.tolist()
    ultimate_issuer_id_index = cols.index("ultimate_issuer_id")
    aladdin_id_index = cols.index("aladdin_id")
    if ultimate_issuer_id_index < aladdin_id_index:
        # Move ultimate_issuer_id to the right of aladdin_id
        cols.insert(aladdin_id_index + 1, cols.pop(ultimate_issuer_id_index))
        overrides = overrides[cols]
    else:
        logger.info("ultimate_issuer_id is already after aladdin_id")

    # save the updated overrides to the output file
    logger.info(f"Saving updated overrides to {output_file}")

    overrides.to_excel(output_file, index=False)
    # deactivated_overrides.to_excel(deactivated_overrides_file, index=False)


if __name__ == "__main__":
    main()
    logger.info("Script completed successfully.")
