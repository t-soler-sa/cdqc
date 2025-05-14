# 02_apply_ovr.py
import warnings
import argparse
import os
from pathlib import Path

import pandas as pd

from scripts.utils.dataloaders import (
    load_overrides,
    load_clarity_data,
    load_crossreference,
)
from scripts.utils.config import get_config

import sys

# print(sys.path)
# print(">>> Running the REAL _02_apply_ovr.py from:", __file__)


# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 0. CONFIGURATION & I/O PATHS
# Get the common configuration for the generator of override list for SAM BAU Infinity
config = get_config("02-apply-ovr-to-dfs", interactive=False, gen_output_dir=False)
logger = config["logger"]
DATE = config["DATE"]
paths = config["paths"]
SRI_DATA_DIR = config["SRI_DATA_DIR"]
OVR_PATH = paths["OVR_PATH"]
DF_PATH = paths["CURRENT_DF_WOUTOVR_PATH"]
DF_SEC_PATH = paths["CURRENT_DF_WOUTOVR_SEC_PATH"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]
OUT_DIR = paths["DF_WOVR_PATH_DIR"]


# 2. Define Functions
def apply_ovr(
    df: pd.DataFrame,
    overrides_df: pd.DataFrame,
    output_suffix: str,
    log_matches: bool = False,
) -> Path:
    grouped_ovr = overrides_df.groupby("ovr_target")

    for ovr_target, group in grouped_ovr:
        logger.info(f"Overriding values for column: {ovr_target}")
        unmatched = []
        permid_matched = []

        for _, row in group.iterrows():
            aladdin_id = row["aladdin_id"]
            permid = row["permid"]
            ovr_value = row["ovr_value"]

            aladdin_id_match = df["aladdin_id"] == aladdin_id
            permid_match = df["permid"] == permid

            if aladdin_id_match.any():
                df.loc[aladdin_id_match, ovr_target] = ovr_value
            elif permid_match.any():
                permid_matched.append(permid)
                df.loc[permid_match, ovr_target] = ovr_value
            else:
                unmatched.append((aladdin_id, permid))

        if log_matches:
            if permid_matched:
                logger.info(
                    f"\n{len(permid_matched)} permids found for unmatched aladdin_id entries! for column '{ovr_target}'. Examples (first 5): "
                    + ", ".join(f"permid={pid}" for pid in permid_matched[:5])
                )

            if unmatched:
                logger.warning(
                    f"\n{len(unmatched)} unmatched entries for column '{ovr_target}'. Examples (first 5): "
                    + ", ".join(
                        f"(aladdin_id={aid}, permid={pid})"
                        for aid, pid in unmatched[:5]
                    )
                )

    # Save the updated DataFrame
    output_file = OUT_DIR / f"{DATE}_df_{output_suffix}_level_with_ovr.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Updated DataFrame saved to {output_file}")

    return output_file


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Apply overrides to the datafeed at the issuer and/or security level"
    )
    parser.add_argument(
        "--dfl",
        choices=["issuer", "security"],
        nargs="+",  # Changed "*" to "+" to require at least one argument explicitly
        default=["issuer"],
        help="Specify the datafeed level to apply override: issuer, security, or both",
    )
    parser.add_argument(
        "--rmsec",
        action="store_true",
        help="Remove/delete the datafeed at the security level after splitting by region?",
    )

    # add argument for date
    # This is a positional argument, so it will be optional and can be provided as the last argument
    parser.add_argument(
        "--date",
        nargs="?",
        help="Date in YYYYMM format (positional)",
    )

    return parser


def main():
    args = parse_arguments().parse_args()

    result = None

    if "issuer" in args.dfl:
        # check first that the issuer datafeed exists
        if not os.path.exists(DF_PATH):
            logger.error(f"File not found: {DF_PATH}")
            raise FileNotFoundError(f"File not found: {DF_PATH}")
        else:
            logger.info(
                f"Applying Overrides to Issuers Datafaeed File found: {DF_PATH}"
            )

    if "security" in args.dfl:
        # check first that the security datafeed exists
        if not os.path.exists(DF_SEC_PATH):
            logger.error(f"File not found: {DF_SEC_PATH}")
            raise FileNotFoundError(f"File not found: {DF_SEC_PATH}")
        else:
            logger.info(
                f"Applying Overrides to Securities Datafaeed File found: {DF_SEC_PATH}"
            )

    # 1. LOAD DATA GLOBAL DATA
    logger.info("Loading overrides and crossreference data...")
    overrides_df = load_overrides(
        OVR_PATH,
        target_cols=["permid", "brs_id", "ovr_target", "ovr_value", "ovr_active"],
    )
    overrides_df.rename(columns={"brs_id": "aladdin_id"}, inplace=True)
    crossreference = load_crossreference(CROSSREFERENCE_PATH)
    # remove duplicate and nan permid in crossreference
    logger.info("Removing duplicates and NaN values from crossreference")
    crossreference.drop_duplicates(subset=["permid"], inplace=True)
    crossreference.dropna(subset=["permid"], inplace=True)

    if "issuer" in args.dfl:
        try:
            logger.info("Applying overrides to issuer data...")
            issuer_df = load_clarity_data(DF_PATH)
            # Merge with crossreference to get the aladdin_id
            issuer_df = issuer_df.merge(
                crossreference[["permid", "aladdin_id"]], on="permid", how="left"
            )
            apply_ovr(issuer_df, overrides_df, "issuer", log_matches=True)
            # We don't store the issuer path since we never need to reference it later
        except Exception as e:
            logger.error(f"Error applying overrides to issuer data: {e}")
            raise

    if "security" in args.dfl:
        try:
            logger.info("Applying overrides to security data...")
            security_df = load_clarity_data(DF_SEC_PATH)
            # Merge with crossreference to get the aladdin_id
            security_df = security_df.merge(
                crossreference[["permid", "aladdin_id"]], on="permid", how="left"
            )
            output_path_securities = apply_ovr(security_df, overrides_df, "security")
            result = output_path_securities  # Only return security path when needed
        except Exception as e:
            logger.error(f"Error applying overrides to issuer data: {e}")
            raise

    return result


if __name__ == "__main__":
    logger.info("Starting the override application process...")
    output_path_sec = main()
    # check if main return path to datefeed at the security level
    if output_path_sec is not None and output_path_sec.exists():
        from scripts.utils.split_df_by_region import main as split_datafeed

        logger.info("Splitting security datafeed by region")
        split_datafeed(output_path_sec)
        # if args "--rmdfecurity" true delete datefeed security level
        args = parse_arguments().parse_args()
        if (args.rmsec) and ("security" in args.dfl):
            if os.path.exists(output_path_sec):
                logger.info(f"Removing security datafeed: {output_path_sec}")
                os.remove(output_path_sec)
    logger.info("Override application process completed.")
