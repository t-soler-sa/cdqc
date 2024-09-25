import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)

def get_current_date():
    """Get current date in yyyymm format."""
    return datetime.now().strftime("%Y%m")

def format_date(date_str):
    """Convert yyyymm to yyyymmdd format."""
    return f"{date_str}01"

def get_month_name(date_str):
    """Get month name from yyyymm format."""
    return datetime.strptime(date_str, "%Y%m").strftime("%b")

def update_path(path, old_date, new_date):
    """Update path with new date."""
    return str(path).replace(old_date, new_date)

def create_directory_if_not_exists(path):
    """Create directory if it doesn't exist."""
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created directory: {path}")

def load_data(file_path):
    """Load data from CSV or Excel file."""
    try:
        if file_path.suffix == '.csv':
            return pd.read_csv(file_path, sep=',', dtype='unicode')
        elif file_path.suffix in ['.xlsx', '.xls']:
            return pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None

def process_overwrite(df, ow_df, column_name):
    """Process overwrite for a specific column."""
    df_filtered = df[['ClarityID', column_name]].copy()
    merged = pd.merge(df_filtered, ow_df, how='left', on='ClarityID')
    
    new_column = f"{column_name}_New"
    merged[new_column] = np.where(
        merged[ow_df.columns[1]] != merged[column_name],
        merged[ow_df.columns[1]],
        merged[column_name]
    )
    merged[new_column] = merged[new_column].fillna(merged[column_name])
    
    return merged[[new_column]]

# Main execution
if __name__ == "__main__":
    # Get the date to process
    date_to_process = input("Enter the date to process (yyyymm format) or press Enter for current date: ")
    if not date_to_process:
        date_to_process = get_current_date()
    
    # Format date and get month name
    formatted_date = format_date(date_to_process)
    month_name = get_month_name(date_to_process)
    base_path = Path("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS")
    
    # Update input file path with new date
    df_path = base_path / f"04_Datos Clarity/01_Equities_feed/01_Ficheros_originales (Descarga en bruto)/{formatted_date}_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
    
    # Ensure the directory for the input file exists
    create_directory_if_not_exists(df_path.parent)
    
    df = load_data(df_path)
    if df is None:
        exit(1)
    
    # Process overwrites
    ow_columns = ['cs_001_sec', 'cs_002_ec', 'cs_003_sec', 'str_001_s', 'str_002_ec', 
                  'str_003_ec', 'str_004_asec', 'str_005_ec', 'str_006_sec', 'str_007_sect', 
                  'art_8_basicos', 'str_003b_ec']
    
    ow_dir = base_path / f"04_Datos Clarity/01_Equities_feed/03_Overwrites/{date_to_process}_OVR_{month_name}"
    create_directory_if_not_exists(ow_dir)
    
    for col in ow_columns:
        ow_path = ow_dir / f"{col.upper()}_{date_to_process}.xlsx"
        ow_df = load_data(ow_path)
        if ow_df is not None:
            new_col = process_overwrite(df, ow_df, col)
            df = pd.concat([df, new_col], axis=1)
            df.drop(columns=[col], inplace=True)
            logging.info(f"Processed overwrite for {col}")
    
    # Save processed data
    output_dir = base_path / "04_Datos Clarity/01_Equities_feed/DF_para_python"
    create_directory_if_not_exists(output_dir)
    output_path = output_dir / f"{date_to_process}_DF_{month_name}.csv"
    df.to_csv(output_path, sep=';', index=False)
    logging.info(f"Saved processed data to {output_path}")