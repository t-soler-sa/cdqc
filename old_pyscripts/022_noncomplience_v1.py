import pandas as pd
import numpy as np
import os
import time
from pathlib import Path
from datetime import datetime
import logging
import warnings
from contextlib import contextmanager
from typing import List, Tuple, Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_DIR = r"C:\Users\n740789\Documents\Projects_local"
DATAFEED_DIR = os.path.join(BASE_DIR, "DataSets", "DATAFEED", "datafeeds_with_ow" )
CROSSREF_DIR = os.path.join(BASE_DIR, "DataSets", "crossreference")
CARTERAS_DIR = os.path.join(BASE_DIR, "DataSets", "aladdin_carteras_benchmarks")
OUTPUT_DIR = os.path.join(BASE_DIR, "DataSets", "incumplimientos")
PORTFOLIO_LISTS_FILE = os.path.join(BASE_DIR, "DataSets", "portfolio_strategies", "portfolio_lists.xlsx")

# Ensure output directory exists
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Configuration
DATE = datetime.now().strftime("%Y%m")

@contextmanager
def suppress_openpyxl_warning():
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
        yield

def load_dataframe(filepath: str, **kwargs) -> pd.DataFrame:
    """Load a dataframe from a file with error handling."""
    try:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"The file {filepath} does not exist.")
        
        if filepath.endswith('.csv'):
            return pd.read_csv(filepath, **kwargs)
        elif filepath.endswith('.xlsx'):
            return pd.read_excel(filepath, **kwargs)
        else:
            raise ValueError(f"Unsupported file format for {filepath}. Only CSV and Excel files are supported.")
    
    except pd.errors.EmptyDataError:
        logging.warning(f"The file {filepath} is empty. Returning an empty DataFrame.")
        return pd.DataFrame()
    
    except Exception as e:
        logging.error(f"An error occurred while loading {filepath}: {str(e)}")
        raise

def load_portfolio_lists(filepath: str) -> dict:
    """Load portfolio lists from Excel file."""
    df = load_dataframe(filepath)
    portfolio_lists = {}
    for column in df.columns:
        portfolio_lists[column] = df[column].dropna().tolist()
    return portfolio_lists

def process_strategy(df: pd.DataFrame, strategy: str, column: str, portfolios: list) -> pd.DataFrame:
    """Process data for a specific strategy."""
    logging.info(f"Processing strategy: {strategy}")
    
    # Update required columns to use permId as the main identifier
    required_columns = ['permId', 'IssuerName', 'Portfolio', column]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.warning(f"Missing columns for strategy {strategy}: {missing_columns}")
        # Update column mapping to focus on permId
        column_mapping = {
            'permId': ['permid', 'perm_id', 'perm', 'permid', 'permId'],
            'IssuerName': ['issuername', 'issuer_name', 'issuer'],
            'Portfolio': ['portfolio', 'portfolio_id'],
        }
        
        for missing_col in missing_columns:
            for alt_col in column_mapping.get(missing_col, []):
                if alt_col in df.columns:
                    df = df.rename(columns={alt_col: missing_col})
                    logging.info(f"Renamed column '{alt_col}' to '{missing_col}'")
                    break
            else:
                raise ValueError(f"Could not find a suitable alternative for column '{missing_col}'")
    
    df_filtered = df[df['Portfolio'].isin(portfolios)].copy()
    df_filtered = df_filtered[required_columns]
    df_filtered = df_filtered.drop_duplicates().dropna(subset=[column])
    df_filtered = df_filtered[df_filtered[column] == 'EXCLUDED']
    df_filtered['Strategy'] = strategy
    df_filtered['Date'] = DATE
    return df_filtered

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process a dataframe by renaming columns and filling NA values."""
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    df = df.fillna(0)
    return df.infer_objects(copy=False)

def transform_dataframes(car: pd.DataFrame, xref: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Transform and rename columns for car and xref dataframes."""
    # Transform car dataframe
    car_columns = ['aladdin_id', 'portfolio_id', 'security_description']
    car_column_mapping = {
        'aladdin_id': ['aladdin_id', 'aladdinid', 'aladdin'],
        'portfolio_id': ['portfolio_id', 'portfolioid', 'portfolio'],
        'security_description': ['security_description', 'securitydescription', 'description']
    }
    
    for col in car_columns:
        if col not in car.columns:
            for alt_col in car_column_mapping[col]:
                if alt_col in car.columns:
                    car = car.rename(columns={alt_col: col})
                    logging.info(f"Renamed column '{alt_col}' to '{col}' in car dataframe")
                    break
            else:
                raise ValueError(f"Could not find a suitable alternative for column '{col}' in car dataframe")
    
    car = car.loc[:, car_columns]

    # transform xref dataframe
    xref_columns = ['aladdin_id', 'issuer_name', 'permid']
    xref_column_mapping = {
        'aladdin_id': ['aladdin_id', 'aladdinid', 'aladdin', 'aladdin_issuer'],
        'issuer_name': ['issuer_name', 'issuername', 'issuer'],
        'permid': ['permid', 'perm_id', 'CLARITY_AI', 'clarity_ai'],
    }
    
    for col in xref_columns:
        if col not in xref.columns:
            for alt_col in xref_column_mapping[col]:
                if alt_col in xref.columns:
                    xref = xref.rename(columns={alt_col: col})
                    logging.info(f"Renamed column '{alt_col}' to '{col}' in xref dataframe")
                    break
            else:
                raise ValueError(f"Could not find a suitable alternative for column '{col}' in xref dataframe")
    
    xref = xref.loc[:, xref_columns + (['ClarityID'] if 'ClarityID' in xref.columns else [])]

    # Ensure permid is lowercase
    xref['permid'] = xref['permid'].str.lower()

    return car, xref

def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, on: str, suffixes: Tuple[str, str]) -> pd.DataFrame:
    """Merge two dataframes."""
    merged_df = pd.merge(df1, df2, on=on, suffixes=suffixes)
    
    # Check if the expected columns exist
    issuer_name_xref = f'issuer_name{suffixes[0]}'
    issuer_name_car = f'issuer_name{suffixes[1]}'
    
    if issuer_name_car in merged_df.columns:
        merged_df.drop(issuer_name_car, axis=1, inplace=True)
    
    if issuer_name_xref in merged_df.columns:
        merged_df.rename(columns={issuer_name_xref: 'issuer_name'}, inplace=True)
    elif 'issuer_name' not in merged_df.columns:
        # If neither expected column exists, we'll use the original 'issuer_name' from df1
        merged_df['issuer_name'] = df1['issuer_name']
    
    return merged_df

def main():
    start_time = time.time()
    logging.info("Script started")

    try:
        # Load portfolio lists
        PORTFOLIO_LISTS = load_portfolio_lists(PORTFOLIO_LISTS_FILE)
        logging.info("Portfolio lists loaded successfully")

        # Load data
        logging.info("Loading data files")
        datafeed = load_dataframe(os.path.join(DATAFEED_DIR,  f"{DATE}_df_issuer_level_with_ow.csv"), dtype='unicode')
        xref = load_dataframe(os.path.join(CROSSREF_DIR, f"Aladdin_Clarity_Issuers_{DATE}01.csv"), dtype={'CLARITY_AI': str})
        with suppress_openpyxl_warning():    
            car = load_dataframe(os.path.join(CARTERAS_DIR, f"{DATE}01_snt world_portf_bmks.xlsx"), sheet_name="portfolio_carteras", skiprows=3)

        # In the main function, after loading the datafeed
        #logging.info(f"Datafeed columns: {datafeed.columns.tolist()}")
        logging.info(f"Datafeed shape: {datafeed.shape}")
        
        # Verify column names and log them
        logging.info(f"Columns in xref dataframe: {xref.columns.tolist()}")
        logging.info(f"Columns in car dataframe: {car.columns.tolist()}")

        # Process dataframes
        logging.info("Processing dataframes")
        dfs = [datafeed, xref, car]
        processed_dfs = [process_dataframe(df) for df in dfs]
        datafeed, xref, car = processed_dfs

        # Verify column names after being processed and log them
        logging.info(f"Columns in xref dataframe: {xref.columns.tolist()}")
        logging.info(f"Columns in car dataframe: {car.columns.tolist()}")

        # Transform dataframes
        logging.info("Transforming dataframes")
        car, xref = transform_dataframes(car, xref)

        # After transforming dataframes
        logging.info(f"car dataframe shape: {car.shape}")
        logging.info(f"xref dataframe shape: {xref.shape}")

        # Filter car dataframe based on portfolio lists
        all_portfolios = set(sum(PORTFOLIO_LISTS.values(), []))
        car = car[car['portfolio_id'].isin(all_portfolios)].dropna().copy()

        # Before merging dataframes
        logging.info(f"Unique aladdin_id in car: {car['aladdin_id'].nunique()}")
        logging.info(f"Unique aladdin_id in xref: {xref['aladdin_id'].nunique()}")

        # Merge dataframes
        logging.info("Merging dataframes")
        merged_df = merge_dataframes(xref, car, on='aladdin_id', suffixes=('_xref', '_car'))
        
        # Merge with datafeed
        datafeed = pd.merge(datafeed, merged_df[['permid', 'portfolio_id']], 
                            left_on='permid', right_on='permid', how='left')
        datafeed = datafeed.drop(columns=['permid'])
        datafeed = datafeed.rename(columns={'portfolio_id': 'Portfolio'})

        # After merging dataframes
        logging.info(f"Merged dataframe shape: {merged_df.shape}")

        # After merging with datafeed
        logging.info(f"Merged datafeed shape: {datafeed.shape}")
        #logging.info(f"Merged datafeed columns: {datafeed.columns.tolist()}")

        # Process strategies
        strategies = {
            'STR001': 'str_001_s',
            'STR002': 'str_002_ec',
            'STR003': 'str_003_ec',
            'STR004': 'str_004_asec',
            'STR005': 'str_005_ec',
            'STR006': 'str_006_sec',
            'STR007': 'str_007_sect',
            'CS001': 'cs_001_sec',
            'ART8': 'art_8_basicos',
            'STR003B': 'str_003b_ec'
        }

        results = {}
        for strategy, column in strategies.items():
            results[strategy] = process_strategy(datafeed, strategy, column, PORTFOLIO_LISTS[strategy])

        # Export results to CSV file
        output_file = os.path.join(OUTPUT_DIR, f"Incumplimientos_{DATE}.csv")
        
        # Export the first strategy with headers
        first_strategy = list(results.keys())[0]
        results[first_strategy].to_csv(output_file, index=False, header=True)
        
        # Export the rest of the strategies without headers
        for strategy in list(results.keys())[1:]:
            results[strategy].to_csv(output_file, index=False, mode='a', header=False)
        
        logging.info(f"Results saved to {output_file}")

        # Create grouped results for alternative format
        all_results = pd.concat(results.values(), ignore_index=True)
        grouped_results = all_results.groupby(['Strategy', 'IssuerName', 'permId', 'Date'])['Portfolio'].agg(list).reset_index()
        
        # Rename columns to match desired output
        grouped_results.columns = ['Strategy', 'company', 'permId', 'Date', 'affected portfolios']

        # Add empty columns for 'Resultado', 'Acción', and 'Motivo Exclusión'
        grouped_results['Result'] = ''
        grouped_results['Action'] = ''
        grouped_results['Exclusion Ground'] = ''

        # Reorder columns to match desired output
        column_order = ['Date', 'Strategy', 'company', 'Result', 'Action', 'Exclusion Ground', 'permId', 'affected portfolios']
        grouped_results = grouped_results[column_order]

        # Convert list of portfolios to string
        grouped_results['affected portfolios'] = grouped_results['affected portfolios'].apply(', '.join)

        # Export grouped results to CSV file with alternative format
        alt_output_file = os.path.join(OUTPUT_DIR, f"Incumplimientos_v2_{DATE}.csv")
        grouped_results.to_csv(alt_output_file, index=False)
        
        logging.info(f"Alternative format results saved to {alt_output_file}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    
    end_time = time.time()
    logging.info(f"Script completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()