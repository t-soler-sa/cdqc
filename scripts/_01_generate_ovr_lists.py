# 01_generate_ovr_lists.py
import warnings

import pandas as pd

from scripts.utils.dataloaders import (
    load_overrides,
    load_crossreference,
    load_clarity_data,
)
from scripts.utils.config import get_config
from scripts.utils.filter_log import main as filter_log

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 0. CONFIGURATION & I/O PATHS
# Get the common configuration for the generator of override list for SAM BAU Infinity
config = get_config("01-generate-ovr-lists", interactive=False)
logger = config["logger"]
DATE = config["DATE"]
paths = config["paths"]
SRI_DATA_DIR = config["SRI_DATA_DIR"]
OVR_PATH = paths["OVR_PATH"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]
DF_PATH = paths["CURRENT_DF_WOUTOVR_PATH"]
OUT_DIR = SRI_DATA_DIR / "ovr_lists_sambau_infinity" / DATE
# create OUT_DIR if does not exist
OUT_DIR.mkdir(parents=True, exist_ok=True)


# define aux function if empty values in colum clarityid load crossreference
def id_to_str(s: pd.Series) -> pd.Series:
    """
    Normalise identifier columns to a pandas StringDtype:
    * keep NaN/NA as NA
    * convert floats/ints to integer-looking strings
      (150236668.0 → "150236668")
    * leave existing strings untouched
    """
    out = s.astype("string").str.replace(  # <- guarantees StringDtype, keeps NA
        r"\.0$", "", regex=True
    )  # drop trailing '.0' if any
    return out


# 1. CONSTANTS
overrides_mapping = {
    "STR_001_SEC": "str_001_s",
    "STR_002_SEC": "str_002_ec",
    "STR_003_SEC": "str_003_ec",
    "STR_003B_EC": "str_003b_ec",
    "STR_004_SEC": "str_004_asec",
    "STR_005_SEC": "str_005_ec",
    "STR_006_SEC": "str_006_sec",
    "STR_007_SECT": "str_007_sect",
    "STR_SFDR8_AEC": "art_8_basicos",
    "CS_001_SEC": "cs_001_sec",
    "CS_002_EC": "cs_002_ec",
}

target_cols_override = [
    "clarityid",
    "ovr_target",
    "ovr_value",
    "ovr_active",
    "aladdin_id",
    "permid",
    "permid",
    "issuer_name",
]

# 2. LOAD OVERRIDES
overrides_df = load_overrides(OVR_PATH, target_cols=target_cols_override)


for col in ("clarityid", "permid"):
    # convert col to datatype string
    overrides_df[col] = id_to_str(overrides_df[col])
    overrides_df[col] = overrides_df[col].replace("", pd.NA)

need_clarityid_only = overrides_df["clarityid"].isna() & overrides_df["permid"].notna()
need_permid_and_clid = overrides_df["clarityid"].isna() & overrides_df["permid"].isna()
# ── 2. optional look-ups ───────────────────────────────────────────────────────
if need_clarityid_only.any() or need_permid_and_clid.any():
    # permid → clarityid map
    clarity_df = load_clarity_data(
        DF_PATH,
        target_cols=["clarityid", "permid"],
    )
    clarity_df["permid"] = id_to_str(clarity_df["permid"])
    clarity_df["clarityid"] = id_to_str(clarity_df["clarityid"])

    clr_map = (
        clarity_df.dropna(subset=["permid", "clarityid"])
        .drop_duplicates("permid")
        .set_index("permid")["clarityid"]
    )  # dtype is already String, no floats!

if need_permid_and_clid.any():
    xref_df = load_crossreference(CROSSREFERENCE_PATH)[["aladdin_id", "permid"]]
    xref_df["permid"] = id_to_str(xref_df["permid"])

    permid_map = (
        xref_df.dropna(subset=["permid"])
        .drop_duplicates("aladdin_id")
        .set_index("aladdin_id")["permid"]
    )

    overrides_df.loc[need_permid_and_clid, "permid"] = overrides_df.loc[
        need_permid_and_clid, "aladdin_id"
    ].map(permid_map)

# second pass: permid → clarityid
still_missing_clid = overrides_df["clarityid"].isna() & overrides_df["permid"].notna()
overrides_df.loc[still_missing_clid, "clarityid"] = overrides_df.loc[
    still_missing_clid, "permid"
].map(clr_map)

# ── 3. final normalisation and logging ────────────────────────────────────────
overrides_df["clarityid"] = id_to_str(overrides_df["clarityid"])  # <-- safety net

permids_assigned_to_clarityid = {}
no_clarityid_no_permid = {}
no_clarityid = {}
for _, row in overrides_df.iterrows():
    aladdin_id = row["aladdin_id"]
    issuer_name = row["issuer_name"]
    claritid = row["clarityid"]
    permid = row["permid"]
    if aladdin_id not in (
        permids_assigned_to_clarityid or no_clarityid_no_permid or no_clarityid
    ):

        if pd.isna(claritid):
            if pd.isna(permid):
                no_clarityid_no_permid[aladdin_id] = issuer_name

            else:
                no_clarityid[aladdin_id] = issuer_name
        elif claritid == permid:
            # this means that the permid was assigned to the clarityid
            permids_assigned_to_clarityid[permid] = issuer_name
        else:
            # there is a clarityid and permid and they are different so everything is fine no need to log anything
            pass

for issuser_name, aladdin_id in no_clarityid_no_permid.items():
    logger.warning(
        f"NoClarityNoPermid | NO clarityid & NO permid for {issuser_name} - aladdin_id: {aladdin_id}"
    )
for issuer_name, aladdin_id in no_clarityid.items():
    logger.warning(
        f"NoClarity | NO clarityid for {issuer_name} - aladdin_id: {aladdin_id}) even after lookup"
    )

for permid, issuer_name in permids_assigned_to_clarityid.items():
    logger.warning(
        f"PermidInstead | For {issuer_name} permid {permid}) was assigned instead of clarityid"
    )

del (
    permids_assigned_to_clarityid,
    no_clarityid_no_permid,
    no_clarityid,
)  # free up memory


# 3. Define main function
def main():
    strategies = "\n".join(str(s) for s in overrides_df.ovr_target.unique())
    logger.info(f"Generating overrides lists for strategies:\n{strategies}")
    grouped_ovr = overrides_df.groupby("ovr_target")
    logger.info(f"Grouped overrides by override target")
    for ovr_target, group in grouped_ovr:
        logger.info(f"Processing override target: {ovr_target}")
        try:
            strategy_name = next(
                k for k, v in overrides_mapping.items() if v == ovr_target
            )
        except StopIteration:
            logger.warning(f"Strategy name not found for target: {ovr_target}")
            continue
        # Create a new DataFrame with the desired columns
        df = pd.DataFrame(
            {
                "clarityid": group["clarityid"],
                strategy_name: group["ovr_value"],
            }
        )
        # Save the DataFrame to an Excel file
        output_file = OUT_DIR / f"{strategy_name}_{DATE}.xlsx"
        logger.info(f"Saving {strategy_name} override list to {output_file}")
        df.to_excel(output_file, index=False)
        logger.info(f"Saved {strategy_name} to {output_file}")


if __name__ == "__main__":
    main()
    logger.info("Script finished. Filtering logs")
    filter_log()
