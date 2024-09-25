import numpy as np
import pandas as pd
import os
import time
import logging
import sys
from typing import List, Tuple, Dict
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter, range_boundaries
import warnings
from contextlib import contextmanager

# Set pandas option to avoid downcasting warning
pd.set_option('future.no_silent_downcasting', True)

# Ignore warnings
@contextmanager
def suppress_openpyxl_warning():
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
        yield

# Display number of line for logging statements
class LineNumberLogFormatter(logging.Formatter):
    def format(self, record):
        record.lineno = f"{record.filename}:{record.lineno}"
        return super().format(record)

# Set up logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
formatter = LineNumberLogFormatter('%(asctime)s - %(levelname)s - [%(lineno)s] - %(message)s')
for handler in logging.root.handlers:
    handler.setFormatter(formatter)

# Start timer to check script performance
start = time.time()

DATE_PREV = "202408"
DATE = "202409"

# Constants
BASE_DIR = r"C:\Users\n740789\Documents\Projects_local"
DATAFEED_DIR = os.path.join(BASE_DIR, "DataSets", "DATAFEED", "ficheros_tratados")
CROSSREF_DIR = os.path.join(BASE_DIR, "DataSets", "crossreference")
PORTFOLIO_DIR = os.path.join(BASE_DIR, "DataSets", "aladdin_carteras_benchmarks")
OUTPUT_DIR = os.path.join(BASE_DIR, "DataSets", "overwrites", "analisis_cambios_ovr_test", DATE)


# load portfolio lists for each strategy
PORTFOLIO_LISTS_FILE = os.path.join(BASE_DIR, "DataSets", "committee_portfolios", "portfolio_lists.xlsx")

def load_portfolio_lists(file_path: str) -> Dict[str, List[str]]:
    try:
        df = pd.read_excel(file_path)
        portfolio_lists = {}
        for column in df.columns:
            portfolio_lists[column] = df[column].dropna().tolist()
        return portfolio_lists
    except Exception as e:
        logging.error(f"Error loading portfolio lists: {str(e)}")
        raise

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
        print(f"Warning: The file {filepath} is empty. Returning an empty DataFrame.")
        return pd.DataFrame()
    
    except Exception as e:
        print(f"An error occurred while loading {filepath}: {str(e)}")
        raise

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process a dataframe by renaming columns and handling NaN values."""
    # Rename columns
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    
    # Handle NaN values and infer data types
    df = df.fillna(0)
    df = df.infer_objects(copy=False)
    
    return df

def transform_dataframes(car_1: pd.DataFrame, car_2: pd.DataFrame, xref_1: pd.DataFrame, xref_2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Transform and rename columns for car and xref dataframes."""
    # Transform car dataframes
    car_1 = car_1.loc[:, ["aladdin_id", 'portfolio_id', 'security_description']]
    car_2 = car_2.loc[:, ["aladdin_id", 'portfolio_id', 'security_description']]

    # Rename xref dataframes
    xref_1.rename(columns={'clarity_ai': 'permid', 'aladdin_issuer': 'aladdin_id'}, inplace=True)
    xref_2.rename(columns={'clarity_ai': 'permid', 'aladdin_issuer': 'aladdin_id'}, inplace=True)

    # Select columns for xref dataframes
    xref_1 = xref_1.loc[:, ['aladdin_id', 'issuer_name', 'permid']]
    xref_2 = xref_2.loc[:, ['aladdin_id', 'issuer_name', 'permid']]

    # Drop rows with empty aladdin_id
    car_1 = car_1.dropna(subset=['aladdin_id'])
    car_2 = car_2.dropna(subset=['aladdin_id'])

    return car_1, car_2, xref_1, xref_2

def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, on: str, suffixes: Tuple[str, str]) -> pd.DataFrame:
    """Merge two dataframes."""
    merged_df = pd.merge(df1, df2, on=on, suffixes=suffixes)
    merged_df.drop(f'issuer_name{suffixes[1]}', axis=1, inplace=True)
    merged_df.rename(columns={f'issuer_name{suffixes[0]}': 'issuer_name'}, inplace=True)
    return merged_df

def add_variation_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add variation columns to the dataframe efficiently."""
    before_cols = df.filter(regex='_before$').columns
    after_cols = [col.replace('_before', '_after') for col in before_cols]
    variation_cols = [f"variation_{col.rpartition('_')[0]}" for col in before_cols]
    
    # Create a new DataFrame with all the new columns at once
    new_cols = pd.DataFrame({
        new_col: df[before].ne(df[after])
        for new_col, before, after in zip(variation_cols, before_cols, after_cols)
    }, index=df.index)
    
    # Combine the original DataFrame with the new columns
    return pd.concat([df, new_cols], axis=1)

def sort_col_suffix(df: pd.DataFrame) -> pd.DataFrame:
    """Sort columns by suffix."""
    col_groups = {}
    for col in df.columns:
        col_name, _, suffix = col.rpartition('_')
        col_groups.setdefault(col_name, []).append((suffix, col))

    sorted_cols = ['permid', 'issuer_name']
    sorted_cols.extend([col for col_name in sorted(col_groups.keys()) for _, col in sorted(col_groups[col_name]) if col not in sorted_cols])
    
    return df[sorted_cols]

def process_sustainability_ratings(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process sustainability ratings data."""
    sust_df = merge_dataframes(df1, df2, 'permid', ('_before', '_after'))
    sust_df['sust_rating_variation'] = sust_df['sustainability_rating_before'] != sust_df['sustainability_rating_after']
    return sust_df, sust_df[['permid', 'issuer_name', 'sust_rating_variation']]

def process_exposures(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process exposure data."""
    exposure_df = merge_dataframes(df1, df2, 'permid', ('_before', '_after'))
    exposure_df = sort_col_suffix(exposure_df)
    exposure_df = add_variation_columns(exposure_df)
    exposure_df["max_expo_variation"] = exposure_df.filter(regex='^variation_').any(axis=1)
    return exposure_df, exposure_df[['permid', 'issuer_name', 'max_expo_variation']]

def process_controversies(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process controversies data."""
    controv_df = merge_dataframes(df1, df2, 'permid', ('_before', '_after'))
    controv_df = sort_col_suffix(controv_df)
    controv_df = add_variation_columns(controv_df)
    variation_cols = controv_df.filter(regex='^variation_').columns
    controv_df["controv_variation"] = controv_df[variation_cols].any(axis=1)
    return controv_df, controv_df[['permid', 'issuer_name', 'controv_variation']]

def process_strategies(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process strategies data."""
    strategies_df = merge_dataframes(df1, df2, 'permid', ('_before', '_after'))
    strategies_df = sort_col_suffix(strategies_df)
    strategies_df = add_variation_columns(strategies_df)
    variation_cols = strategies_df.filter(regex='^variation_').columns
    strategies_df["variation_estrategias"] = strategies_df[variation_cols].any(axis=1)
    return strategies_df, strategies_df[['permid', 'issuer_name', 'variation_estrategias']]

def load_portfolio_list(filepath: str) -> List[str]:
    """Load portfolio list from a file."""
    with open(filepath, 'r') as f:
        return [line.strip() for line in f.readlines()]

def filter_portfolios(car: pd.DataFrame, portfolio_list: List[str]) -> pd.DataFrame:
    """Filter portfolios based on the provided list."""
    return car[car['portfolio_id'].isin(portfolio_list)].copy()

def filter_xref(xref: pd.DataFrame, car: pd.DataFrame) -> pd.DataFrame:
    """Filter cross-reference data based on portfolios."""
    filtered = xref[xref['aladdin_id'].isin(car['aladdin_id'])]
    return filtered[filtered['permid'].notna()]

def filter_df(df: pd.DataFrame, xref: pd.DataFrame) -> pd.DataFrame:
    """Filter main dataframe based on cross-reference data."""
    return df[df['permid'].isin(xref['permid'])]

def create_portfolio_dfs(car: pd.DataFrame, xref: pd.DataFrame, portfolio_lists: Dict[str, List[str]]) -> Dict[str, pd.DataFrame]:
    """Create portfolio dataframes for each strategy."""
    portfolio_dfs = {}
    for strategy, portfolio_list in portfolio_lists.items():
        portfolio = car[car['portfolio_id'].isin(portfolio_list)]
        portfolio = pd.merge(portfolio, xref, on='aladdin_id')
        portfolio_dfs[strategy] = portfolio[['permid']].drop_duplicates()
    return portfolio_dfs

def process_data(df1: pd.DataFrame, df2: pd.DataFrame, xref1: pd.DataFrame, xref2: pd.DataFrame, 
                 car1: pd.DataFrame, car2: pd.DataFrame, portfolio_lists: Dict[str, List[str]]) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Process all data and return results for each strategy."""
    # Load portfolio list
    portfolio_list = load_portfolio_list(os.path.join(BASE_DIR, "DataSets", "carteras_list.txt"))
    
    # Filter portfolios
    car1_strat = filter_portfolios(car1, portfolio_list)
    car2_strat = filter_portfolios(car2, portfolio_list)
    
    # Filter cross-reference data
    xref1_filtered = filter_xref(xref1, car1_strat)
    xref2_filtered = filter_xref(xref2, car2_strat)
    
    # Filter main dataframes
    df1_filtered = filter_df(df1, xref1_filtered)
    df2_filtered = filter_df(df2, xref2_filtered)
    
    # Create portfolio dataframes for each strategy
    portfolio_dfs = create_portfolio_dfs(car2, xref2, portfolio_lists)
    
    # Process different aspects of the data
    sust_df, sust_summary = process_sustainability_ratings(df1_filtered, df2_filtered)
    exposure_df, exposure_summary = process_exposures(df1_filtered, df2_filtered)
    controv_df, controv_summary = process_controversies(df1_filtered, df2_filtered)
    strategies_df, strategies_summary = process_strategies(df1_filtered, df2_filtered)
    
    # Combine all summaries
    all_summaries = [
        (sust_summary, 'sust_rating_variation'),
        (controv_summary, 'controv_variation'),
        (strategies_summary, 'variation_estrategias'),
        (exposure_summary, 'max_expo_variation')
    ]
    
    combined_summary = exposure_summary.copy()
    for summary, col in all_summaries[1:]:
        combined_summary = pd.merge(combined_summary, summary[['permid', col]], on='permid', how='outer')
    
    combined_summary['variation_df'] = combined_summary.filter(regex='^variation_').any(axis=1)
    
    # Create result dataframes for each strategy
    results = {}
    for strategy, portfolio in portfolio_dfs.items():
        strategy_data = pd.merge(combined_summary, portfolio, on='permid')
        strategy_results = {
            'Resumen': strategy_data,
            'Sust_R': pd.merge(sust_df[sust_df['sust_rating_variation']], portfolio, on='permid'),
            'Max_Exp': pd.merge(exposure_df[exposure_df['max_expo_variation']], portfolio, on='permid'),
            'Controversias': pd.merge(controv_df[controv_df['controv_variation']], portfolio, on='permid'),
            'Estrategias': pd.merge(strategies_df[strategies_df['variation_estrategias']], portfolio, on='permid')
        }
        results[strategy] = strategy_results
    
    return results

def export_results(results: dict, output_dir: str):
    """Export results to Excel files."""
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    for file_name, sheets in results.items():
        full_path = os.path.join(output_dir, f"{file_name}_v5.xlsx")
        with pd.ExcelWriter(full_path) as writer:
            for sheet_name, data in sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
    logging.info(f"Results exported to {output_dir}")

def main():
    start_time = time.time()
    logging.info("Script started")

    try:
        # Load portfolio lists
        logging.info("Loading portfolio lists")
        PORTFOLIO_LISTS = load_portfolio_lists(PORTFOLIO_LISTS_FILE)

        # Load data
        logging.info("Loading data files")
        df_1 = load_dataframe(os.path.join(DATAFEED_DIR, f"{DATE_PREV}01_Equities_feed_IssuerLevel_sinOVR.csv"), dtype='unicode')
        df_2 = load_dataframe(os.path.join(DATAFEED_DIR, f"{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"), dtype='unicode')
        xref_1 = load_dataframe(os.path.join(CROSSREF_DIR, f"Aladdin_Clarity_Issuers_{DATE_PREV}01.csv"), dtype={'CLARITY_AI': str})
        xref_2 = load_dataframe(os.path.join(CROSSREF_DIR, f"Aladdin_Clarity_Issuers_{DATE}01.csv"), dtype={'CLARITY_AI': str})
        with suppress_openpyxl_warning():    
            car_1 = load_dataframe(os.path.join(PORTFOLIO_DIR, f"{DATE_PREV}01_snt world_portf_bmks.xlsx"), sheet_name="portfolio_carteras", skiprows=3)
            car_2 = load_dataframe(os.path.join(PORTFOLIO_DIR, f"{DATE}01_snt world_portf_bmks.xlsx"), sheet_name="portfolio_carteras", skiprows=3)

        # Process dataframes
        logging.info("Processing dataframes")
        dfs = [df_1, df_2, xref_1, xref_2, car_1, car_2]
        processed_dfs = [process_dataframe(df) for df in dfs]
        df_1, df_2, xref_1, xref_2, car_1, car_2 = processed_dfs

        # Transform dataframes
        logging.info("Transforming dataframes")
        car_1, car_2, xref_1, xref_2 = transform_dataframes(car_1, car_2, xref_1, xref_2)

        # Process data
        logging.info("Processing data")
        results = process_data(df_1, df_2, xref_1, xref_2, car_1, car_2, PORTFOLIO_LISTS)

        # Export results
        logging.info("Exporting results")
        export_results(results, OUTPUT_DIR)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    
    end_time = time.time()
    logging.info(f"Script completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()