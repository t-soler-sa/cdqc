import pandas as pd
import logging
import time
from pathlib import Path
from contextlib import contextmanager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Timer context manager
@contextmanager
def timer(description: str):
    start = time.time()
    yield
    elapsed_time = time.time() - start
    logging.info(f"{description} took {elapsed_time:.2f} seconds")

def get_user_input():
    while True:
        date_input = input("Enter the date in YYYYMM format: ")
        if len(date_input) == 6 and date_input.isdigit():
            return date_input
        else:
            print("Invalid input. Please enter a 6-digit number in YYYYMM format.")

# Get user input for date
DATE = get_user_input()

# Constants
DATAFEED_PATH = Path(rf'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\raw_dataset\{DATE}01_Production\{DATE}01_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv')
OW_BASE_PATH = Path(rf'C:\Users\n740789\Documents\Projects_local\DataSets\overwrites\{DATE}_OVR_permid')
OUTPUT_PATH = Path(r'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ow')

# Define overwrites
overwrites = [
    (f'CS_001_SEC_{DATE}.xlsx', 'cs_001_sec', 'CS_001_SEC'),
    (f'CS_002_EC_{DATE}.xlsx', 'cs_002_ec', 'CS_002_EC'),
    (f'CS_003_SEC_{DATE}.xlsx', 'cs_003_sec', 'CS_003_SEC'),
    (f'STR_001_SEC_{DATE}.xlsx', 'str_001_s', 'STR_001_SEC'),
    (f'STR_002_SEC_{DATE}.xlsx', 'str_002_ec', 'STR_002_SEC'),
    (f'STR_003_SEC_{DATE}.xlsx', 'str_003_ec', 'STR_003_SEC'),
    (f'STR_004_SEC_{DATE}.xlsx', 'str_004_asec', 'STR_004_SEC'),
    (f'STR_005_SEC_{DATE}.xlsx', 'str_005_ec', 'STR_005_SEC'),
    (f'STR_006_SEC_{DATE}.xlsx', 'str_006_sec', 'STR_006_SEC'),
    (f'STR_007_SECT_{DATE}.xlsx', 'str_007_sect', 'STR_007_SECT'),
    (f'STR_SFDR8_AEC_{DATE}.xlsx', 'art_8_basicos', 'STR_SFDR8_AEC'),
    (f'STR_003B_EC_{DATE}.xlsx', 'str_003b_ec', 'STR_003B_EC')
]

def load_main_dataframe():
    logging.info("Start loading of dataframe")
    with timer("Loading main dataframe"):
        df = pd.read_csv(DATAFEED_PATH, low_memory=False)
        logging.info(f"Loaded main dataframe with shape: {df.shape}")
        
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Ensure 'permid' column exists and is read as string
        id_column = 'permid' if 'permid' in df.columns else 'permID'
        df[id_column] = df[id_column].astype(str)
        
        return df

def apply_overwrites(df, overwrites):
    id_column = 'permid' if 'permid' in df.columns else 'permID'
    
    for file_name, original_col, update_col in overwrites:
        with timer(f"Applying overwrite for {original_col}"):
            file_path = OW_BASE_PATH / file_name
            
            if not file_path.exists():
                logging.warning(f"Overwrite file not found: {file_path}")
                continue
            
            ow_df = pd.read_excel(file_path)
            ow_df.columns = ow_df.columns.str.lower()
            
            # Ensure 'permid' column in overwrite file is read as string
            ow_df['permid'] = ow_df['permid'].astype(str)
            
            # Check if the update column exists in the overwrite file
            if update_col.lower() not in ow_df.columns:
                logging.warning(f"Column {update_col} not found in {file_name}. Skipping this overwrite.")
                continue
            
            # Create a mapping dictionary from PermID to the new value
            update_map = dict(zip(ow_df['permid'], ow_df[update_col.lower()]))
            
            # Update the original column with new values where available
            mask = df[id_column].isin(update_map.keys())
            df.loc[mask, original_col] = df.loc[mask, id_column].map(update_map)
            
            logging.info(f"Applied overwrite for {original_col}. Updated {mask.sum()} rows.")
    
    return df

def main():
    with timer("Total execution time"):
        # Ensure output directory exists
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        
        # Load main dataframe
        df = load_main_dataframe()
        
        # Apply overwrites
        df = apply_overwrites(df, overwrites)
        
        # Save updated dataframe
        output_file = OUTPUT_PATH / f"{DATE}01_datafeed_with_ow.csv"
        with timer("Saving updated dataframe"):
            df.to_csv(output_file, sep=',', index=False)
            logging.info(f"Saved updated dataframe to {output_file}")

if __name__ == "__main__":
    main()