import pandas as pd
import time
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

def main():
    # Reading the Excel file
    file_path = "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/BBDD_Overrides_may24.xlsx"
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
        "STR001": "OVRSTR001",
        "STR002": "OVRSTR002",
        "STR003": "OVRSTR003",
        "STR003B": "OVRSTR003B",
        "STR004": "OVRSTR004",
        "STR005": "OVRSTR005",
        "STR006": "OVRSTR006",
        "STR007": "OVRSTR007",
        "CS001": "OVRCS001",
        "CS002": "OVRCS002",
        "CS003": "OVRCS003",
        "ARTICULO 8": "OVRARTICULO8"
    }
    
    for new_name, column_name in overwrites.items():
        df_filtered = filter_and_rename(BBDD_Overwrites, column_name, new_name)
        file_path = f"X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/{new_name.replace(' ', '_')}_SEC_202406.xlsx"
        write_to_excel(df_filtered, file_path)

if __name__ == "__main__":
    main()