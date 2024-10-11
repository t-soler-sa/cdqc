import pandas as pd
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

test_col = [
    "str_001_s","str_002_ec","str_003_ec","str_004_asec","str_005_ec","cs_001_sec","gp_esccp",
    "cs_003_sec","cs_002_ec","str_006_sec","str_007_sect","gp_esccp_22","gp_esccp_25","gp_esccp_30",
    "art_8_basicos","str_003b_ec"
]

# Define constants 
DATE_PREV = "202409"
DATE = "202410"

# Read files
df_1_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE_PREV}01_Equities_feed_IssuerLevel_sinOVR.csv"
df_2_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"

# Read only the necessary columns
columns_to_read = ['permid', 'isin', 'issuer_name'] + test_col
df_1 = pd.read_csv(df_1_path, sep=',', dtype='unicode', usecols=columns_to_read)
df_2 = pd.read_csv(df_2_path, sep=',', dtype='unicode', usecols=columns_to_read)

logging.info(f"df_1 shape: {df_1.shape}, df_2 shape: {df_2.shape}")

# Ensure both DataFrames have the same index (permId)
df_1.set_index('permid', inplace=True)
df_2.set_index('permid', inplace=True)

# Get the common indexes
common_indexes = df_1.index.intersection(df_2.index)
logging.info(f"Number of common indexes: {len(common_indexes)}")

# Filter both DataFrames to only include common indexes
df_1 = df_1.loc[common_indexes]
df_2 = df_2.loc[common_indexes]

# Initialize delta DataFrame with common indexes and all columns from df_2
delta = df_2.copy()

# Compare the DataFrames
for col in test_col:
    if col in df_1.columns and col in df_2.columns:
        logging.info(f"Comparing column: {col}")
        # Find rows where the values are different
        diff_mask = (df_1[col] != df_2[col])
        
        # Keep only the different values in delta
        delta.loc[~diff_mask, col] = np.nan

# Add the new_exclusion column
delta['new_exclusion'] = False

# Function to get the list of columns that changed to EXCLUDED
def get_exclusion_list(row):
    return [col for col in test_col if row[col] == 'EXCLUDED' and df_1.loc[row.name, col] != 'EXCLUDED']

# Check for new exclusions and create exclusion_list
for col in test_col:
    if col in df_1.columns and col in df_2.columns:
        logging.info(f"Checking for new exclusions in column: {col}")
        mask = (df_1[col] != 'EXCLUDED') & (df_2[col] == 'EXCLUDED')
        delta.loc[mask, 'new_exclusion'] = True
        logging.info(f"Number of new exclusions in {col}: {mask.sum()}")

# Create exclusion_list column
delta['exclusion_list'] = delta.apply(get_exclusion_list, axis=1)

# Remove rows where all test columns are NaN (no changes)
delta = delta.dropna(subset=test_col, how='all')

# Reset index to make permId a column again
delta.reset_index(inplace=True)

logging.info(f"Final delta shape: {delta.shape}")

# Display the first few rows of the result
print(delta.head())

# Optional: Save to CSV
delta.to_csv('delta_results.csv', index=False)

logging.info("Analysis completed successfully.")