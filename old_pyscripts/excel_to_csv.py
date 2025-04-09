import pandas as pd
import os
from pathlib import Path


def excel_to_csv_per_sheet(excel_file_path):
    downloads_path = Path.home() / "Downloads"
    xls = pd.ExcelFile(excel_file_path)

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        safe_sheet_name = "".join(c if c.isalnum() else "_" for c in sheet_name)
        csv_file_path = downloads_path / f"{safe_sheet_name}.csv"
        df.to_csv(csv_file_path, index=False)
        print(f"✅ Saved: {csv_file_path}")


if __name__ == "__main__":
    excel_file = "/Users/tristan_soler/projects/c-dq-controls/notebooks/20250402_pre_ovr_analysis.xlsx"
    excel_to_csv_per_sheet(excel_file)
