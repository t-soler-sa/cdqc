import pandas as pd
import os
import sys
from datetime import datetime
from openpyxl import Workbook

def read_excel(file_path, sheet_name):
    """Read the specified sheet from an Excel file into a DataFrame."""
    return pd.read_excel(file_path, sheet_name=sheet_name)

def filter_and_rename(df, column_name, new_name):
    """Filter DataFrame rows based on specified column values and rename the column."""
    df_filtered = df[df[column_name].isin(["OK", "FLAG", "EXCLUDED"])]
    df_filtered = df_filtered[["ClarityID", column_name]].rename(columns={column_name: new_name})
    return df_filtered

def write_to_excel(df, path):
    """Write DataFrame to an Excel file."""
    df.to_excel(path, index=False)

def validate_year_month(year_month):
    """Validate the input year_month format."""
    try:
        datetime.strptime(year_month, "%Y%m")
        return True
    except ValueError:
        return False

def get_year_month():
    while True:
        user_input = input("Please insert date with the format yyyymm: ")
        if validate_year_month(user_input):
            return user_input
        else:
            print("Sorry, wrong format. Please try again.")

def main(year_month=None):
    if not year_month:
        year_month = get_year_month()
    elif not validate_year_month(year_month):
        print("Error: The date format is incorrect. Please use 'yyyymm' format.")
        return

    # Reading the Excel file
    file_path = "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/BBDD_Overrides_ago24.xlsx"


    sheet_name = 'MAPEO P-S'
    BBDD_Overwrites = read_excel(file_path, sheet_name)

    # Renaming columns
    columns = ["#", "ClarityID", "PermID", "IssuerID", "IssuerName", "SNTWorld", "ParentNameAladdin", "ParentID",
               "ParSub", "ParentNameClarity", "ParentIdClarity", "GICS2", "GICS_2 Parent", "Sustainability Rating Parent",
               "OVR Inheritance BiC", "OVRSTR001", "OVRSTR002", "OVRSTR003", "OVRSTR003B", "OVRSTR004", "OVRSTR005",
               "OVRSTR006", "OVRSTR007", "OVRCS001", "OVRCS002", "OVRCS003", "OVRARTICULO8", "MotivoPrinc", "MotivoSec", "Detalle"]
    BBDD_Overwrites.columns = columns

    # Filtering and renaming columns for each subset
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

    current_year = year_month[:4]
    current_month = datetime.strptime(year_month, "%Y%m").strftime('%B')

    # Create the directory for the current month if it doesn't exist
    directory = f"X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/{year_month}_OVR_{current_month}"
    os.makedirs(directory, exist_ok=True)

    for new_name, column_name in overwrites.items():
        df_filtered = filter_and_rename(BBDD_Overwrites, column_name, new_name)
        file_path = f"{directory}/{new_name}_{year_month}.xlsx"
        write_to_excel(df_filtered, file_path)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()