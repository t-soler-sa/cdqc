import os
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Tuple, Union
import warnings
from contextlib import contextmanager
import logging
import time

# Constants
DATE = "202407"
DATAFEED_PATH = Path(r'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\raw_dataset\20240701_Production\20240701_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv')
CROSSREF_DIR = Path(r'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference')
PORTFOLIO_DIR = Path(r'C:\Users\n740789\Documents\Projects_local\DataSets\aladdin_carteras_benchmarks')
OW_BASE_PATH = Path(r'C:\Users\n740789\Documents\Projects_local\DataSets\overwrites\202407_OVR_July')
OUTPUT_PATH = Path(r'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ow')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@contextmanager
def timer(description: str):
    start = time.time()
    yield
    elapsed_time = time.time() - start
    logging.info(f"{description} took {elapsed_time:.2f} seconds")

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
        
        if filepath.suffix == '.csv':
            return pd.read_csv(filepath, **kwargs)
        elif filepath.suffix in ['.xlsx', '.xls']:
            return pd.read_excel(filepath, **kwargs)
        else:
            raise ValueError(f"Unsupported file format for {filepath}. Only CSV and Excel files are supported.")
    
    except pd.errors.EmptyDataError:
        logging.warning(f"The file {filepath} is empty. Returning an empty DataFrame.")
        return pd.DataFrame()
    
    except Exception as e:
        logging.error(f"An error occurred while loading {filepath}: {str(e)}")
        raise

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process a dataframe by renaming columns and standardizing column names."""
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    
    # Ensure consistent naming for 'permid' column
    permid_columns = ['permid', 'permId', 'PermID', 'perm_id', 'Perm_ID']
    for col in permid_columns:
        if col in df.columns:
            df = df.rename(columns={col: 'permid'})
            break
    
    return df.infer_objects(copy=False)

def transform_dataframes(car: pd.DataFrame, xref: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Transform and rename columns for car and xref dataframes."""
    # Transform car dataframe
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

    return car, xref

def load_ow_file(file_name):
    """Load an overwrite file."""
    ow_df = load_dataframe(OW_BASE_PATH / file_name)
    logging.info(f"Columns in {file_name}: {list(ow_df.columns)}")
    return ow_df

def apply_overwrite(df, ow_df, column_name, ow_column_name):
    """Apply overwrite to the main dataframe using permId as the identifier."""
    # log relevant columns from the main dataframe
    relevant_columns = ['permid', 'permId', 'PermID', 'perm_id', 'Perm_ID', column_name]
    present_columns = [col for col in relevant_columns if col in df.columns]
    logging.info(f"Relevant columns in main dataframe: {present_columns}")

    # log columns of ow_df
    logging.info(f"Columns in overwrite dataframe before renaming: {list(ow_df.columns)}")

    # Ensure both dataframes have 'permid' column
    for dataframe in [df, ow_df]:
        permid_columns = ['permid', 'permId', 'PermID', 'perm_id', 'Perm_ID']
        for col in permid_columns:
            if col in dataframe.columns:
                dataframe = dataframe.rename(columns={col: 'permid'}, inplace=True)
                logging.info(f"Renamed '{col}' to 'permid' in dataframe")
                break

    logging.info(f"Columns in overwrite dataframe after renaming: {list(ow_df.columns)}")

    if 'permid' not in df.columns:
        logging.error("permid column not found in the main dataframe")
        return df[column_name] if column_name in df.columns else pd.Series(index=df.index)
    
    if 'permid' not in ow_df.columns:
        logging.error("permid column not found in the overwrite dataframe")
        return df[column_name]
    
    if column_name not in df.columns:
        logging.error(f"{column_name} column not found in the main dataframe")
        return pd.Series(index=df.index)
    
    # Ensure ow_column_name exists in ow_df
    if ow_column_name not in ow_df.columns:
        logging.error(f"{ow_column_name} column not found in the overwrite dataframe")
        return df[column_name]
    
    merged = pd.merge(df[['permid', column_name]], ow_df[['permid', ow_column_name]], how='left', on='permid')
    new_column = np.where(merged[ow_column_name].notna() & (merged[ow_column_name] != merged[column_name]), 
                          merged[ow_column_name], 
                          merged[column_name])
    return pd.Series(new_column, index=df.index)

def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Process a single chunk of the dataframe."""
    return process_dataframe(chunk)

def main():
    start_time = time.time()

    with timer("Loading and processing data file in chunks"):
        chunksize = 100000  # Adjust this value based on your available memory
        chunks = []
        
        for chunk in pd.read_csv(DATAFEED_PATH, chunksize=chunksize, dtype='unicode', low_memory=False):
            processed_chunk = process_chunk(chunk)
            chunks.append(processed_chunk)
            logging.info(f"Processed chunk of size {len(chunk)}")
        
        df = pd.concat(chunks, ignore_index=True)
        logging.info(f"Finished processing all chunks. Total rows: {len(df)}")
    

    # Ensure 'permid' column is present and correctly named in the main dataframe
    if 'permid' not in df.columns:
        permid_columns = ['permId', 'PermID', 'perm_id', 'Perm_ID']
        for col in permid_columns:
            if col in df.columns:
                df = df.rename(columns={col: 'permid'})
                logging.info(f"Renamed '{col}' to 'permid' in the main dataframe")
                break
        else:
            logging.error("No 'permid' column found in the main dataframe")

    with timer("Loading other data files"):
        xref = load_dataframe(CROSSREF_DIR / f"Aladdin_Clarity_Issuers_{DATE}01.csv", dtype={'CLARITY_AI': str})
        with suppress_openpyxl_warning():    
            car = load_dataframe(PORTFOLIO_DIR / f"{DATE}01_snt world_portf_bmks.xlsx", sheet_name="portfolio_carteras", skiprows=3)

    with timer("Transforming dataframes"):
        car, xref = transform_dataframes(car, xref)

    # Define overwrites
    overwrites = [
        ('CS_001_SEC_202407.xlsx', 'cs_001_sec', 'CS_001_SEC'),
        ('CS_002_EC_202407.xlsx', 'cs_002_ec', 'CS_002_EC'),
        ('CS_003_SEC_202407.xlsx', 'cs_003_sec', 'CS_003_SEC'),
        ('STR_001_SEC_202407.xlsx', 'str_001_s', 'STR_001_SEC'),
        ('STR_002_SEC_202407.xlsx', 'str_002_ec', 'STR_002_SEC'),
        ('STR_003_SEC_202407.xlsx', 'str_003_ec', 'STR_003_SEC'),
        ('STR_004_SEC_202407.xlsx', 'str_004_asec', 'STR_004_SEC'),
        ('STR_005_SEC_202407.xlsx', 'str_005_ec', 'STR_005_SEC'),
        ('STR_006_SEC_202407.xlsx', 'str_006_sec', 'STR_006_SEC'),
        ('STR_007_SECT_202407.xlsx', 'str_007_sect', 'STR_007_SECT'),
        ('STR_SFDR8_AEC_202407.xlsx', 'art_8_basicos', 'STR_SFDR8_AEC'),
        ('STR_003B_EC_202407.xlsx', 'str_003b_ec', 'STR_003B_EC')
    ]

    with timer("Applying overwrites"):
        for file_name, column_name, ow_column_name in overwrites:
            with timer(f"Applying overwrite for {file_name}"):
                ow_df = load_ow_file(file_name)
                df[column_name] = apply_overwrite(df, ow_df, column_name, ow_column_name)

    with timer("Saving the result"):
        output_file = OUTPUT_PATH / f"{DATE}01_datafeed_with_ow.csv"
        df.to_csv(output_file, sep=',', index=False)
        logging.info(f"Data saved to {output_file}")

    total_time = time.time() - start_time
    logging.info(f"Total execution time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()