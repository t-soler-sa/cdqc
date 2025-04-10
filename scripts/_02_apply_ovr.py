# 02_apply_ovr.py
import warnings

import pandas as pd

from utils.dataloaders import load_overrides, load_clarity_data
from utils.config import get_config

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 0. CONFIGURATION & I/O PATHS
# Get the common configuration for the generator of override list for SAM BAU Infinity
config = get_config("01-generate_ovr_lists", interactive=False)
logger = config["logger"]
DATE = config["DATE"]
paths = config["paths"]
SRI_DATA_DIR = config["SRI_DATA_DIR"]
OVR_PATH = paths["OVR_PATH"]
DF_PATH = paths["CURRENT_DF_WOUTOVR_PATH"]
OUT_DIR = paths["DF_WOVR_PATH_DIR"]

# 1. LOAD DATA
overrides_df = load_overrides(
    OVR_PATH, target_cols=["permid", "ovr_target", "ovr_value"]
)
df = load_clarity_data(DF_PATH)


# 2. Define main function
def main():
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
    output_file = OUT_DIR / f"{DATE}_df_issuer_level_with_ovr.csv"
    df.to_csv(output_file, index=False)

    logger.info(f"Updated DataFrame saved to {output_file}")


if __name__ == "__main__":
    main()
