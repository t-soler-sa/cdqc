# 02_apply_ovr.py
import warnings
import argparse
import os
from pathlib import Path

import pandas as pd

from scripts.utils.dataloaders import load_overrides, load_clarity_data
from scripts.utils.config import get_config

import sys

# print(sys.path)
# print(">>> Running the REAL _02_apply_ovr.py from:", __file__)


# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 0. CONFIGURATION & I/O PATHS
# Get the common configuration for the generator of override list for SAM BAU Infinity
config = get_config("01-generate_ovr_lists", interactive=False, gen_output_dir=False)
logger = config["logger"]
DATE = config["DATE"]
paths = config["paths"]
SRI_DATA_DIR = config["SRI_DATA_DIR"]
OVR_PATH = paths["OVR_PATH"]
DF_PATH = paths["NEW_DF_WOVR_PATH"]
DF_SEC_PATH = paths["CURRENT_DF_WOUTOVR_SEC_PATH"]
OUT_DIR = paths["DF_WOVR_PATH_DIR"]

# 1. LOAD DATA GLOBAL DATA
overrides_df = load_overrides(
    OVR_PATH, target_cols=["permid", "ovr_target", "ovr_value", "ovr_active"]
)


# 2. Define Functions
def apply_ovr(df: pd.DataFrame, output_suffix: str) -> Path:
    grouped_ovr = overrides_df.groupby("ovr_target")
    for ovr_target, group in grouped_ovr:
        logger.info(f"Overriding values for column: {ovr_target}")
        for _, row in group.iterrows():
            # Find the matching row(s) in df
            permid_match = df["permid"] == row["permid"]
            if permid_match.any():
                df.loc[permid_match, ovr_target] = row["ovr_value"]
            else:
                logger.warning(
                    f"No match found for permid {row['permid']} in main df for column {ovr_target}"
                )

    # Save the updated DataFrame to a new CSV file
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
        default=["issuer", "security"],
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
        logger.info("Applying overrides to issuer data...")
        issuer_df = load_clarity_data(DF_PATH)
        apply_ovr(issuer_df, "issuer")
        # We don't store the issuer path since we never need to reference it later

    if "security" in args.dfl:
        logger.info("Applying overrides to security data...")
        security_df = load_clarity_data(DF_SEC_PATH)
        output_path_securities = apply_ovr(security_df, "security")
        result = output_path_securities  # Only return security path when needed

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
        if args.rmsec:
            if os.path.exists(output_path_sec):
                logger.info(f"Removing security datafeed: {output_path_sec}")
                os.remove(output_path_sec)
    logger.info("Override application process completed.")
