#!/usr/bin/env python
"""
Overrides ClarityID Processing Script

This script processes an Excel file containing overrides information,
filters data based on certain column criteria, and writes out separate Excel
files for each override type.

Usage:
    python 01_overrides_clarityid.py [date]
If no argument is provided, the script will prompt for a valid date in YYYYMM format.
"""

import sys
import time
from datetime import datetime
from pathlib import Path
import pandas as pd

# Import configuration, dataloaders, and get_date functions
from config import get_config
from utils.dataloaders import load_excel
from utils.get_date import get_date

# Using logger from configuration
logger = None  # Will be set after loading config

VALID_VALUES = ["OK", "FLAG", "EXCLUDED"]


def filter_and_rename(
    df: pd.DataFrame, column_name: str, new_name: str
) -> pd.DataFrame:
    """
    Filter DataFrame rows based on specified column values and rename the column.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): Column to filter.
        new_name (str): New name for the column.

    Returns:
        pd.DataFrame: Filtered and renamed DataFrame.
    """
    df_filtered = df[df[column_name].isin(VALID_VALUES)][["ClarityID", column_name]]
    return df_filtered.rename(columns={column_name: new_name})


def write_to_excel(df: pd.DataFrame, path: Path):
    """Write DataFrame to an Excel file."""
    logger.info(f"Writing to Excel file: {path}")
    try:
        df.to_excel(path, index=False)
    except Exception as e:
        logger.exception(f"Failed to write to Excel file: {path}")
        raise e


def process_overrides(year_month: str, base_dir: Path, input_file: Path):
    """
    Process the overrides Excel file by filtering and renaming columns,
    and writing separate output files for each override type.
    """
    sheet_name = "MAPEO P-S"
    logger.info(f"Reading sheet '{sheet_name}' from {input_file}")
    overwrites_df = load_excel(input_file, sheet_name)

    # Define expected columns and assign them
    expected_columns = [
        "#",
        "ClarityID",
        "PermID",
        "IssuerID",
        "IssuerName",
        "SNTWorld",
        "ParentNameAladdin",
        "ParentID",
        "ParSub",
        "ParentNameClarity",
        "ParentIdClarity",
        "GICS2",
        "GICS_2 Parent",
        "Sustainability Rating Parent",
        "OVR Inheritance BiC",
        "OVRSTR001",
        "OVRSTR002",
        "OVRSTR003",
        "OVRSTR003B",
        "OVRSTR004",
        "OVRSTR005",
        "OVRSTR006",
        "OVRSTR007",
        "OVRCS001",
        "OVRCS002",
        "OVRCS003",
        "OVRARTICULO8",
        "MotivoPrinc",
        "MotivoSec",
        "Detalle",
        "Fecha aplicación OVR",
    ]
    overwrites_df.columns = expected_columns

    # Mapping of new override names to original column names
    overrides_mapping = {
        "STR_001_SEC": "OVRSTR001",
        "STR_002_SEC": "OVRSTR002",
        "STR_003_SEC": "OVRSTR003",
        "STR_003B_EC": "OVRSTR003B",
        "STR_004_SEC": "OVRSTR004",
        "STR_005_SEC": "OVRSTR005",
        "STR_006_SEC": "OVRSTR006",
        "STR_007_SECT": "OVRSTR007",
        "CS_001_SEC": "OVRCS001",
        "CS_002_EC": "OVRCS002",
        "CS_003_SEC": "OVRCS003",
        "STR_SFDR8_AEC": "OVRARTICULO8",
    }

    current_month = datetime.strptime(year_month, "%Y%m").strftime("%B")
    output_base = base_dir / f"{year_month}_OVR_{current_month}_clarityid"
    output_base.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory set to: {output_base}")

    for new_name, orig_column in overrides_mapping.items():
        df_filtered = filter_and_rename(overwrites_df, orig_column, new_name)
        output_file = output_base / f"{new_name}_{year_month}.xlsx"
        write_to_excel(df_filtered, output_file)


def main():
    # Load configuration and logger
    config = get_config("01_overrides_clarityid", gen_output_dir=False)
    global logger
    logger = config["logger"]

    start_time = time.time()
    logger.info("Script started")

    # Use config to determine the base directory for overrides files.
    base_dir = config["SRI_DATA_DIR"] / "overrides"

    # Get the date (YYYYMM) via get_date from utils.
    year_month = get_date()

    # Construct the expected input file path.
    input_file = base_dir / f"{year_month}_BBDD_Overrides.xlsx"

    if not input_file.exists():
        logger.error(f"Input file {input_file} does not exist.")
        sys.exit(1)

    process_overrides(year_month, base_dir, input_file)

    end_time = time.time()
    logger.info(f"Script completed in {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
