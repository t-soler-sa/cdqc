import pandas as pd
import os
import sys
import time
from datetime import datetime
from pathlib import Path
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_excel(file_path: Path, sheet_name: str) -> pd.DataFrame:
    """Read the specified sheet from an Excel file into a DataFrame."""
    logging.info(f"Reading Excel file: {file_path}, sheet: {sheet_name}")
    return pd.read_excel(file_path, sheet_name=sheet_name)

def filter_and_rename(df: pd.DataFrame, column_name: str, new_name: str) -> pd.DataFrame:
    """Filter DataFrame rows based on specified column values and rename the column."""
    valid_values = ["OK", "FLAG", "EXCLUDED"]
    df_filtered = df[df[column_name].isin(valid_values)][["PermID", column_name]]
    return df_filtered.rename(columns={column_name: new_name})

def write_to_excel(df: pd.DataFrame, path: Path):
    """Write DataFrame to an Excel file."""
    logging.info(f"Writing to Excel file: {path}")
    df.to_excel(path, index=False)

def validate_year_month(year_month: str) -> bool:
    """Validate the input year_month format."""
    try:
        datetime.strptime(year_month, "%Y%m")
        return True
    except ValueError:
        return False

def get_year_month() -> str:
    while True:
        user_input = input("Please insert date with the format yyyymm: ")
        if validate_year_month(user_input):
            return user_input
        logging.warning("Invalid date format entered. Please try again.")

def process_overwrites(year_month: str, base_dir: Path, input_file: Path):
    if not validate_year_month(year_month):
        logging.error("Error: The date format is incorrect. Please use 'yyyymm' format.")
        return

    sheet_name = 'MAPEO P-S'
    overwrites_df = read_excel(input_file, sheet_name)

    columns = [
        "#", "ClarityID", "PermID", "IssuerID", "IssuerName", "SNTWorld", "ParentNameAladdin", "ParentID",
        "ParSub", "ParentNameClarity", "ParentIdClarity", "GICS2", "GICS_2 Parent", "Sustainability Rating Parent",
        "OVR Inheritance BiC", "OVRSTR001", "OVRSTR002", "OVRSTR003", "OVRSTR003B", "OVRSTR004", "OVRSTR005",
        "OVRSTR006", "OVRSTR007", "OVRCS001", "OVRCS002", "OVRCS003", "OVRARTICULO8", "MotivoPrinc", "MotivoSec", "Detalle"
    ]
    overwrites_df.columns = columns

    overwrites = {
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
        "STR_SFDR8_AEC": "OVRARTICULO8"
    }

    current_month = datetime.strptime(year_month, "%Y%m").strftime('%B')
    directory = base_dir / f"{year_month}_OVR_{current_month}"
    directory.mkdir(parents=True, exist_ok=True)

    for new_name, column_name in overwrites.items():
        df_filtered = filter_and_rename(overwrites_df, column_name, new_name)
        file_path = directory / f"{new_name}_{year_month}.xlsx"
        write_to_excel(df_filtered, file_path)

def main():
    setup_logging()
    start_time = time.time()
    logging.info("Script started")
    base_dir = Path(r"C:\Users\n740789\Documents\Projects_local\DataSets\overwrites")
    input_file = base_dir / "BBDD_Overrides_sep24.xlsx"

    if len(sys.argv) > 1:
        year_month = sys.argv[1]
    else:
        year_month = get_year_month()

    process_overwrites(year_month, base_dir, input_file)

    end_time = time.time()
    logging.info(f"Script completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()