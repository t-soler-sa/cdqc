import pandas as pd
import numpy as np
from pathlib import Path
import os
from functools import partial
import logging
from typing import Union, Tuple, Dict
import warnings
from contextlib import contextmanager

# Constants
DATE = "202407"
BASE_DIR = Path(r"C:\Users\n740789\Documents\Projects_local")
DATAFEED_PATH = BASE_DIR / "DataSets" / "DATAFEED" / "datafeeds_with_ow" / f"{DATE}_df_issuer_level_with_ow.csv"
CARTERAS_DIR = BASE_DIR / "DataSets" / "aladdin_carteras_benchmarks"
CROSSREF_DIR = BASE_DIR / "DataSets" / "crossreference"
OUTPUT_DIR = BASE_DIR / "DataSets" / "incumplimientos"
PORTFOLIO_LISTS_FILE = BASE_DIR / "DataSets" / "portfolio_strategies" / "portfolio_lists.xlsx"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@contextmanager
def suppress_openpyxl_warning():
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
        yield

def load_dataframe(filepath: Union[str, Path], **kwargs) -> pd.DataFrame:
    """Load a dataframe from a file with error handling."""
    try:
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"The file {filepath} does not exist.")
        
        # Combine dtype specifications
        dtype_spec = kwargs.pop('dtype', {})
        if 'permid' not in dtype_spec:
            dtype_spec['permid'] = str
        
        if filepath.suffix == '.csv':
            df = pd.read_csv(filepath, dtype=dtype_spec, **kwargs)
        elif filepath.suffix in ['.xlsx', '.xls']:
            with suppress_openpyxl_warning():
                df = pd.read_excel(filepath, dtype=dtype_spec, **kwargs)
        else:
            raise ValueError(f"Unsupported file format for {filepath}. Only CSV and Excel files are supported.")
        
        logging.info(f"Successfully loaded {filepath}")
        logging.info(f"DataFrame shape: {df.shape}")
        
        # Log only the first 10 column names if there are more than 10
        if len(df.columns) > 10:
            logging.info(f"First 10 DataFrame columns: {df.columns[:10].tolist()}")
            logging.info(f"... and {len(df.columns) - 10} more columns")
        else:
            logging.info(f"DataFrame columns: {df.columns.tolist()}")
        
        return df
    
    except pd.errors.EmptyDataError:
        logging.warning(f"The file {filepath} is empty. Returning an empty DataFrame.")
        return pd.DataFrame()
    
    except Exception as e:
        logging.error(f"An error occurred while loading {filepath}: {str(e)}")
        raise

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process a dataframe by renaming columns and standardizing column names."""
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    
    # Ensure consistent naming for 'permid' column and convert to string
    permid_columns = ['permid', 'permId', 'PermID', 'perm_id', 'Perm_ID']
    for col in permid_columns:
        if col in df.columns:
            df = df.rename(columns={col: 'permid'})
            df['permid'] = df['permid'].astype(str)
            break
    
    return df.infer_objects(copy=False)


def transform_dataframes(car: pd.DataFrame, xref: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Transform and rename columns for car and xref dataframes."""
    logging.info("Transforming car and xref dataframes")

    # Transform car dataframe
    car_original_columns = car.columns.tolist()
    car = car.rename(columns={
        'Issuer Name': 'issuer_name',
        'Filter Level 1': 'filter_level_1',
        'Level': 'level',
        'aladdin_id': 'aladdin_id',
        'Security Description': 'security_description',
        'Portfolio Full Name': 'portfolio_full_name',
        'portfolio_id': 'portfolio_id'
    })
    car = car.loc[:, ["aladdin_id", 'portfolio_id', 'security_description']]
    car = car.dropna(subset=['aladdin_id'])
    logging.info(f"Car original columns: {car_original_columns}")
    logging.info(f"Car transformed columns: {car.columns.tolist()}")

    # Transform xref dataframe
    xref = xref.rename(columns={
        'Aladdin_Issuer': 'aladdin_issuer',
        'Issuer_Name': 'issuer_name',
        'CLARITY_AI': 'clarity_ai',
        'MSCI': 'msci',
        'SUST': 'sust'
    })
    xref = xref.rename(columns={'clarity_ai': 'permid', 'aladdin_issuer': 'aladdin_id'})
    xref = xref.loc[:, ['aladdin_id', 'issuer_name', 'permid']]
    xref['permid'] = xref['permid'].astype(str)

    return car, xref

def load_data():
    """Load and process all required data."""
    logging.info("Loading data...")
    
    # Load datafeed
    df = load_dataframe(DATAFEED_PATH)
    df = process_dataframe(df)
    
    # Load carteras
    car_file = CARTERAS_DIR / f"{DATE}01_snt world_portf_bmks.xlsx"
    with suppress_openpyxl_warning():
        car = load_dataframe(car_file, sheet_name="portfolio_carteras", skiprows=3)
    
    # Load crossreference
    xref_file = CROSSREF_DIR / f"Aladdin_Clarity_Issuers_{DATE}01.csv"
    xref = load_dataframe(xref_file)  # Remove the dtype argument here
    
    # Transform car and xref
    car, xref = transform_dataframes(car, xref)
    
    logging.info("Data loading complete.")
    return df, car, xref

def prepare_data(df: pd.DataFrame, car: pd.DataFrame, xref: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Prepare data for processing strategies."""
    logging.info("Preparing data...")
    
    # Filter columns in df
    columns_to_keep = ['permid', 'str_001_s', 'str_002_ec', 'str_003_ec', 'str_004_asec', 'str_005_ec', 
                       'str_006_sec', 'str_007_sect', 'cs_001_sec', 'cs_002_ec', 'cs_003_sec', 
                       'art_8_basicos', 'gp_esccp_22', 'str_003b_ec']
    df_filtered = df[columns_to_keep]
    
    # Merge car and xref
    carteras_merged = pd.merge(car, xref, on='aladdin_id', how='left')
    
    logging.info("Data preparation complete.")
    return df_filtered, carteras_merged, xref

def process_strategy(df_filtered: pd.DataFrame, carteras_merged: pd.DataFrame, strategy: str, portfolios: list, column_name: str) -> pd.DataFrame:
    """Process a single strategy."""
    logging.info(f"Processing strategy: {strategy}")
    
    strategy_df = carteras_merged[carteras_merged['portfolio_id'].isin(portfolios)].copy()
    
    df_strategy = df_filtered[['permid', column_name]].copy().rename(columns={column_name: strategy})
    df_strategy['permid'] = df_strategy['permid'].astype(str)
    
    strategy_df = pd.merge(strategy_df, df_strategy, on='permid', how='left')
    strategy_df = strategy_df.drop_duplicates().dropna(subset=[strategy])
    
    strategy_df = strategy_df[strategy_df[strategy] == 'EXCLUDED']
    strategy_df = strategy_df[['aladdin_id', 'issuer_name', 'portfolio_id', strategy]]
    
    return strategy_df

def load_portfolio_lists() -> pd.DataFrame:
    """Load portfolio lists from Excel file."""
    logging.info("Loading portfolio lists...")
    try:
        portfolio_lists = pd.read_excel(PORTFOLIO_LISTS_FILE, sheet_name='Sheet1')
        logging.info(f"Columns in portfolio lists: {portfolio_lists.columns.tolist()}")
        return portfolio_lists
    except Exception as e:
        logging.error(f"Error loading portfolio lists: {str(e)}")
        raise

def get_portfolio_list(portfolio_lists: pd.DataFrame, strategy: str) -> list:
    """Get portfolio list for a specific strategy."""
    try:
        return portfolio_lists[strategy].dropna().tolist()
    except KeyError:
        logging.warning(f"Strategy '{strategy}' not found in portfolio lists. Returning empty list.")
        return []

def main():
    df, car, xref = load_data()
    df_filtered, carteras_merged, xref = prepare_data(df, car, xref)
    
    portfolio_lists = load_portfolio_lists()
    
    strategies = {
        'STR001': ('str_001_s', portfolio_lists['STR001'].dropna().tolist()),
        'STR002': ('str_002_ec', portfolio_lists['STR002'].dropna().tolist()),
        'STR003': ('str_003_ec', portfolio_lists['STR003'].dropna().tolist()),
        'STR004': ('str_004_asec', portfolio_lists['STR004'].dropna().tolist()),
        'STR003B': ('str_003b_ec', portfolio_lists['STR003B'].dropna().tolist()),
        'STR005': ('str_005_ec', portfolio_lists['STR005'].dropna().tolist()),
        'STR006': ('str_006_sec', portfolio_lists['STR006'].dropna().tolist()),
        'STR007': ('str_007_sect', portfolio_lists['STR007'].dropna().tolist()),
        'CS001': ('cs_001_sec', portfolio_lists['CS001'].dropna().tolist()),
        'ART8': ('art_8_basicos', portfolio_lists['ART8'].dropna().tolist()),
    }
    
    results = []
    for strategy, (column_name, portfolios) in strategies.items():
        if portfolios:
            result = process_strategy(df_filtered, carteras_merged, strategy, portfolios, column_name)
            results.append(result)
        else:
            logging.warning(f"Skipping strategy {strategy} due to empty portfolio list.")
    
    if not results:
        logging.error("No results to save. Check if any portfolio lists were successfully loaded.")
        return

    # Combine all results and save to CSV
    output_file = OUTPUT_DIR / f'Incumplimientos_{DATE}.csv'
    pd.concat(results).to_csv(output_file, sep=',', index=False)
    logging.info(f"Results saved to {output_file}")

if __name__ == '__main__':
    main()