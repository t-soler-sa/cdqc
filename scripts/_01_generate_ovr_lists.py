# 01_generate_ovr_lists.py
import warnings

import pandas as pd

from utils.dataloaders import load_overrides
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
OUT_DIR = SRI_DATA_DIR / "ovr_lists_sambau_infinity" / DATE
# create OUT_DIR if does not exist
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. CONSTANTS
overrides_mapping = {
    "STR_001_SEC": "str_001_s",
    "STR_002_SEC": "str_002_ec",
    "STR_003_SEC": "str_003_ec",
    "STR_003B_EC": "str_003b_ec",
    "STR_004_SEC": "str_004_asec",
    "STR_005_SEC": "str_005_ec",
    "STR_006_SEC": "str_006_sec",
    "STR_SFDR8_AEC": "art_8_basicos",
    "CS_001_SEC": "cs_001_sec",
    "CS_002_EC": "cs_002_ec",
}

# 2. LOAD OVERRIDES
overrides_df = load_overrides(
    OVR_PATH, target_cols=["clarityid", "ovr_target", "ovr_value"]
)


# 3. Define main function
def main():
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
    logger.info("Script finished")
