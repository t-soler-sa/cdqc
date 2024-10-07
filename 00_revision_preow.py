import pandas as pd
import numpy as np
import os
import time
import re
import logging
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter, range_boundaries
import warnings
import sys

# Suppress the specific UserWarning from openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Start timer to check script performance
start = time.time()

# Constants
#DATE_PREV = "202409"
#DATE = "202410"

# ask input for data in format YYYYMM
DATE = input("Please insert date with the format yyyymm: ")
# using DATE define DATE_PREV previous month data in format YYYYMM
DATE_PREV = str(int(DATE) - 1)

# log DATE and DATE_PREV
logging.info(f"CURRENT DATE: {DATE} PREVIOUS MONTH: {DATE_PREV}")

# for loop and ask for input to follow or to input date again
while True:
    follow = input("Do you want to continue with the date? (Y/N): ")
    if follow.upper() == "Y":
        break
    elif follow.upper() == "N":
        # give option to quit
        quit = input("Do you want to quit? (Y/N): ")
        if quit.upper() == "Y":
            logging.info("Quitting the script")
            sys.exit()
        
        DATE = input("Please insert date with the format yyyymm: ")
        DATE_PREV = str(int(DATE) - 1)
        logging.info(f"DATE: {DATE}")
    else:
        logging.info("Please insert Y or N")

# Function to create output directory if it doesn't exist
def create_output_directory(base_dir):
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        logging.info(f"Created output directory: {base_dir}")
    else:
        logging.info(f"Output directory already exists: {base_dir}")

# Function to sort columns by suffix
def sort_col_suffix(df):
    col_groups = {}
    for col in df.columns:
        col_name, _, suffix = col.rpartition('_')
        col_groups.setdefault(col_name, []).append((suffix, col))

    sorted_cols = []
    for col_name in sorted(col_groups.keys()):
        sorted_cols.extend(col for _, col in sorted(col_groups[col_name]))

    df_sorted = df[sorted_cols]
    df_sorted = df_sorted.reindex(sorted(df_sorted.columns), axis=1)

    permid_col = df_sorted.pop("permid")
    df_sorted.insert(0, "permid", permid_col)

    column_order = ['permid','issuer_name'] + [col for col in df_sorted.columns if col not in ['permid','issuer_name']]

    return df_sorted[column_order]

def add_variation_columns(df):
    logging.info(f"Starting add_variation_columns. DataFrame shape: {df.shape}")
    
    before_cols = df.filter(regex='_before$').columns
    after_cols = [col.replace('_before', '_after') for col in before_cols]
    variation_cols = ['variation_' + col.rpartition('_')[0] for col in before_cols]
    
    # Only create variation columns for existing pairs of before/after columns
    valid_cols = [i for i, col in enumerate(after_cols) if col in df.columns]
    before_cols = [before_cols[i] for i in valid_cols]
    after_cols = [after_cols[i] for i in valid_cols]
    variation_cols = [variation_cols[i] for i in valid_cols]
    
    # Create variation columns
    variation_data = {variation: df[before].ne(df[after]) for before, after, variation in zip(before_cols, after_cols, variation_cols)}
    variation_df = pd.DataFrame(variation_data, index=df.index)
    
    # Concatenate the original dataframe with the variation dataframe
    result_df = pd.concat([df, variation_df], axis=1)
    
    # Reorder columns
    new_order = []
    for col in df.columns:
        if col.endswith('_before'):
            base_name = col.rpartition('_')[0]
            variation_col = f'variation_{base_name}'
            if variation_col in result_df.columns:
                new_order.extend([variation_col, col])
            else:
                new_order.append(col)
        elif not (col.endswith('_after') or col.startswith('variation_')):
            new_order.append(col)
    new_order.extend([col for col in result_df.columns if col.endswith('_after')])
    
    result_df = result_df[new_order]
    logging.info(f"Finished add_variation_columns. Resulting DataFrame shape: {result_df.shape}")
    return result_df

# Main script execution
logging.info("Starting script execution")

# Read input files
logging.info("Reading input files")
df_1_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE_PREV}01_Equities_feed_IssuerLevel_sinOVR.csv"
df_2_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"

df_1 = pd.read_csv(df_1_path, sep=',', dtype='unicode')
df_2 = pd.read_csv(df_2_path, sep=',', dtype='unicode')

xref_1 = pd.read_csv(rf'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_{DATE_PREV}01.csv', dtype={'CLARITY_AI': str})
xref_2 = pd.read_csv(rf'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_{DATE}01.csv', dtype={'CLARITY_AI': str})

car_1 = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\aladdin_carteras_benchmarks\{DATE_PREV}01_snt world_portf_bmks.xlsx', sheet_name = "portfolio_carteras", skiprows=3)
car_2 = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\aladdin_carteras_benchmarks\{DATE}01_snt world_portf_bmks.xlsx', sheet_name = "portfolio_carteras", skiprows=3)

# Transform column names
list_dfs = [car_1, car_2, xref_1, xref_2]
for df in list_dfs:
    df.columns = df.columns.str.lower().str.strip().str.replace(" ","_")

df_1.columns = df_1.columns.str.lower().str.strip().str.replace(" ","_")
df_2.columns = df_2.columns.str.lower().str.strip().str.replace(" ","_")

# Transform and prepare datasets
logging.info("Transforming and preparing datasets")
car_1 = car_1.loc[:, ["aladdin_id", 'portfolio_id', 'security_description']]
car_2 = car_2.loc[:, ["aladdin_id", 'portfolio_id', 'security_description']]

xref_1.rename(columns={'clarity_ai':'permid', 'aladdin_issuer': 'aladdin_id'}, inplace=True)
xref_2.rename(columns={'clarity_ai':'permid', 'aladdin_issuer': 'aladdin_id'}, inplace=True)

xref_1 = xref_1.loc[:, ['aladdin_id', 'issuer_name', 'permid']]
xref_2 = xref_2.loc[:, ['aladdin_id', 'issuer_name', 'permid']]

car_1 = car_1.dropna(subset=['aladdin_id'])
car_2 = car_2.dropna(subset=['aladdin_id'])

# Read portfolio list
logging.info("Reading portfolio list")
with open(r'C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\carteras_list.txt', 'r') as f:
    carteras_list = [line.strip() for line in f.readlines()]

car_1_strat = car_1[car_1['portfolio_id'].isin(carteras_list)].copy()
car_2_strat = car_2[car_2['portfolio_id'].isin(carteras_list)].copy()

# Cross-reference and filter
logging.info("Performing cross-reference and filtering")
xref_1_filtered = xref_1.loc[xref_1['aladdin_id'].isin(car_1_strat['aladdin_id'])]
xref_1_filtered = xref_1_filtered[xref_1_filtered['permid'].notna()]

xref_2_filtered = xref_2.loc[xref_2['aladdin_id'].isin(car_2_strat['aladdin_id'])]
xref_2_filtered = xref_2_filtered[xref_2_filtered['permid'].notna()]

df_1_filtered = df_1.loc[df_1['permid'].isin(xref_1_filtered['permid'])]
df_2_filtered = df_2.loc[df_2['permid'].isin(xref_2_filtered['permid'])]

# Define strategies
logging.info("Defining strategies")

# Path to the Excel file
excel_path = r"C:\Users\n740789\Documents\Projects_local\DataSets\committee_portfolios\portfolio_lists.xlsx"

try:
    # Read the Excel file
    df_strategies = pd.read_excel(excel_path, sheet_name='Sheet1')
    
    # Create a dictionary to store strategies
    strategies = {}
    
    # Iterate through columns (strategies) in the DataFrame
    for column in df_strategies.columns:
        # Remove any NaN values and convert to list
        strategy_list = df_strategies[column].dropna().tolist()
        strategies[column] = strategy_list

    # Assign strategies to variables
    lista_str001 = strategies.get('STR001', [])
    lista_str002 = strategies.get('STR002', [])
    lista_str003 = strategies.get('STR003', [])
    lista_str003b = strategies.get('STR003B', [])
    lista_str004 = strategies.get('STR004', [])
    lista_str005 = strategies.get('STR005', [])
    lista_art8 = strategies.get('ART8', [])
    lista_cs001 = strategies.get('CS001', [])

    logging.info("Strategies successfully defined from Excel file")

except FileNotFoundError:
    logging.error(f"Excel file not found: {excel_path}")
    raise
except Exception as e:
    logging.error(f"Error reading Excel file: {str(e)}")
    raise

# Use the strategy lists to create portfolio DataFrames
portfolio_str001 = car_2[car_2['portfolio_id'].isin(lista_str001)]
portfolio_str002 = car_2[car_2['portfolio_id'].isin(lista_str002)]
portfolio_str003 = car_2[car_2['portfolio_id'].isin(lista_str003)]
portfolio_str003b = car_2[car_2['portfolio_id'].isin(lista_str003b)]
portfolio_str004 = car_2[car_2['portfolio_id'].isin(lista_str004)]
portfolio_str005 = car_2[car_2['portfolio_id'].isin(lista_str005)]
portfolio_art8 = car_2[car_2['portfolio_id'].isin(lista_art8)]
portfolio_cs001 = car_2[car_2['portfolio_id'].isin(lista_cs001)]

# Merge with XREF
portfolio_str001 = pd.merge(portfolio_str001, xref_2, on='aladdin_id')
portfolio_str002 = pd.merge(portfolio_str002, xref_2, on='aladdin_id')
portfolio_str003 = pd.merge(portfolio_str003, xref_2, on='aladdin_id')
portfolio_str003b = pd.merge(portfolio_str003b, xref_2, on='aladdin_id')
portfolio_str004 = pd.merge(portfolio_str004, xref_2, on='aladdin_id')
portfolio_str005 = pd.merge(portfolio_str005, xref_2, on='aladdin_id')
portfolio_art8 = pd.merge(portfolio_art8, xref_2, on='aladdin_id')
portfolio_cs001 = pd.merge(portfolio_cs001, xref_2, on='aladdin_id')

# Filter merged df with xref on col permid
portfolio_str001 = portfolio_str001[['permid']]
portfolio_str002 = portfolio_str002[['permid']]
portfolio_str003 = portfolio_str003[['permid']]
portfolio_str003b = portfolio_str003b[['permid']]
portfolio_str004 = portfolio_str004[['permid']]
portfolio_str005 = portfolio_str005[['permid']]
portfolio_art8 = portfolio_art8[['permid']]
portfolio_cs001 = portfolio_cs001[['permid']]

# Drop duplicates
portfolio_str001 = portfolio_str001.drop_duplicates()
portfolio_str002 = portfolio_str002.drop_duplicates()
portfolio_str003 = portfolio_str003.drop_duplicates()
portfolio_str003b = portfolio_str003b.drop_duplicates()
portfolio_str004 = portfolio_str004.drop_duplicates()
portfolio_str005 = portfolio_str005.drop_duplicates()
portfolio_art8 = portfolio_art8.drop_duplicates()
portfolio_cs001 = portfolio_cs001.drop_duplicates()

logging.info("Portfolio DataFrames created and processed")

# Process variations for each dimension
logging.info("Processing variations for each dimension")

# Sustainability ratings
logging.info("Processing sustainability ratings")

# Select Relevant Fields/Columns
df_1_sustainability_ratings = df_1_filtered.loc[:, ['issuer_name', 'permid', 'sustainability_rating']]
df_2_sustainability_ratings = df_2_filtered.loc[:, ['issuer_name', 'permid', 'sustainability_rating']]

# Merge df of both months on permid
sustainability_ratings_df = pd.merge(df_1_sustainability_ratings, df_2_sustainability_ratings, on='permid', suffixes=('_before', '_after'))
sustainability_ratings_df.drop(['issuer_name_after'], axis='columns', inplace=True)
sustainability_ratings_df.rename(columns={'issuer_name_before': 'issuer_name'}, inplace=True)
sustainability_ratings_df = sustainability_ratings_df[['permid', 'issuer_name', 'sustainability_rating_before', 'sustainability_rating_after']]

# Create Variation Column
sustainability_ratings_df['sust_rating_variation'] = sustainability_ratings_df.apply(
    lambda row: row['sustainability_rating_before'] != row['sustainability_rating_after'], axis=1
)

# Select the variation column for the final dataframe
sustainability_rating = sustainability_ratings_df[['permid', 'issuer_name', 'sust_rating_variation']]

# Create a df with the portfolios that changed
sust_rat_variation_true = sustainability_ratings_df[sustainability_ratings_df['sust_rating_variation'] == True]

# Define the order of those columns
sust_rat_variation_true = sust_rat_variation_true[['permid', 'issuer_name', 'sust_rating_variation', 'sustainability_rating_before', 'sustainability_rating_after']]

# Merge those equities with variations with their strategies
sust_rat_variation_true_str001 = pd.merge(sust_rat_variation_true, portfolio_str001, on='permid')
sust_rat_variation_true_str002 = pd.merge(sust_rat_variation_true, portfolio_str002, on='permid')
sust_rat_variation_true_str003 = pd.merge(sust_rat_variation_true, portfolio_str003, on='permid')
sust_rat_variation_true_str003b = pd.merge(sust_rat_variation_true, portfolio_str003b, on='permid')
sust_rat_variation_true_str004 = pd.merge(sust_rat_variation_true, portfolio_str004, on='permid')
sust_rat_variation_true_str005 = pd.merge(sust_rat_variation_true, portfolio_str005, on='permid')
sust_rat_variation_true_art8 = pd.merge(sust_rat_variation_true, portfolio_art8, on='permid')
sust_rat_variation_true_cs001 = pd.merge(sust_rat_variation_true, portfolio_cs001, on='permid')

logging.info("Sustainability ratings processing completed")

# Exposure
logging.info("Processing exposure")

# Define exposure column list
exposure_column_list = [
    'issuer_name', 'permid', 'maxexp_abortifacents_part', 'maxexp_alcohol_part', 'maxexp_alchol_prod',
    'maxexp_antip_landmines_part', 'maxexp_armament_part', 'maxexp_armament_prod',
    'maxexp_chemic_biolog_weapons_part', 'maxexp_cluster_bombs_part', 'maxexp_coal_mining_part',
    'maxexp_coal_mining_prod', 'maxexp_coal_power_gen_part', 'maxexp_coal_power_gen_prod',
    'maxexp_contraceptives_part', 'maxexp_embryonic_stem_cell_research_part',
    'maxexp_fossil_fuels_part', 'maxexp_fossil_fuels_prod', 'maxexp_gambling_part',
    'maxexp_gambling_prod', 'maxexp_gmo_products_part', 'maxexp_gmo_research_part',
    'maxexp_nuclear_weapons_part', 'maxexp_oil_sands_part', 'maxexp_oil_sands_prod',
    'maxexp_palm_oil_part', 'maxexp_pornography_part', 'maxexp_shale_energy_part',
    'maxexp_shale_energy_prod', 'maxexp_stem_cell_research', 'maxexp_tobacco_part',
    'maxexp_tobacco_prod', 'maxexp_artic_oil_part', 'maxexp_small_arms_part',
    'maxexp_white_phosporus_part', 'maxexp_nuclear_weapons_prod', 'maxexp_cluster_bombs_prod',
    'maxexp_thermal_coal_prod', 'maxexp_gas_fuels_prod', 'maxexp_gas_fuels_part',
    'maxexp_oil_fuels_prod', 'maxexp_oil_fuels_part'
]

# Filter to create exposure dfs
exposure_1 = df_1_filtered.loc[:, exposure_column_list]
exposure_2 = df_2_filtered.loc[:, exposure_column_list]

# Merge these expose dfs based on permid
exposure_df = pd.merge(exposure_1, exposure_2, on='permid', suffixes=('_before', '_after'))

# Apply transformations to the new merged df
exposure_df.drop(['issuer_name_after'], axis='columns', inplace=True)
exposure_df.rename(columns={'issuer_name_before': 'issuer_name'}, inplace=True)

# Fill empty values with "0"
exposure_df = exposure_df.fillna(0)

# Order Columns
exposure_df = sort_col_suffix(exposure_df)

# Select columns/fields relevant to exposure
exposure_columns = [col for col in exposure_df.columns if col.startswith('maxexp_') or col in ['permid', 'issuer_name']]
exposure_df = exposure_df[exposure_columns]

# Transform certain values to integers
numeric_columns = [col for col in exposure_df.columns if col not in ['permid', 'issuer_name']]
exposure_df[numeric_columns] = exposure_df[numeric_columns].astype(float).astype(int)

# Create Variation Columns
exposure_df = add_variation_columns(exposure_df)

variation_cols = [col for col in exposure_df.columns if col.startswith("variation_")]
exposure_df["max_expo_variation"] = exposure_df[variation_cols].any(axis=1)

# Reorder columns
column_order = ['permid', 'issuer_name', 'max_expo_variation']
variation_columns = [col for col in exposure_df.columns if col.startswith('variation_')]
other_columns = sorted(set(exposure_df.columns) - set(column_order) - set(variation_columns))
exposure_df = exposure_df[column_order + variation_columns + other_columns]

# Select the Columns with the variation that will be added to the final Dataframe
exposures = exposure_df[['permid', 'issuer_name', 'max_expo_variation']]

# Merge those equities with variations with their strategies
exposure_variation_true = exposure_df[exposure_df['max_expo_variation'] == True]

exposure_variation_true_str001 = pd.merge(exposure_variation_true, portfolio_str001, on='permid')
exposure_variation_true_str002 = pd.merge(exposure_variation_true, portfolio_str002, on='permid')
exposure_variation_true_str003 = pd.merge(exposure_variation_true, portfolio_str003, on='permid')
exposure_variation_true_str003b = pd.merge(exposure_variation_true, portfolio_str003b, on='permid')
exposure_variation_true_str004 = pd.merge(exposure_variation_true, portfolio_str004, on='permid')
exposure_variation_true_str005 = pd.merge(exposure_variation_true, portfolio_str005, on='permid')
exposure_variation_true_art8 = pd.merge(exposure_variation_true, portfolio_art8, on='permid')
exposure_variation_true_cs001 = pd.merge(exposure_variation_true, portfolio_cs001, on='permid')

logging.info("Exposure processing completed")

# Controversies
logging.info("Processing controversies")

# Define controversies column list
controversies_column_list = [
    'issuer_name', 'permid', 'any_critical_controversy', 'controv_access_to_basic_services',
    'controv_accounting_and_taxation', 'controv_animal_welfare', 'controv_animal_welfare_sc',
    'controv_anticompetitive_practices', 'controv_bribery_and_corruption',
    'controv_bribery_and_corruption_sc', 'controv_business_ethics', 'controv_business_ethics_sc',
    'controv_carbon_impact_of_products', 'controv_community_relations',
    'controv_community_relations_sc', 'controv_corporate_governance',
    'controv_data_privacy_and_security', 'controv_emissions_effluents_and_waste',
    'controv_emissions_effluents_and_waste_sc', 'controv_employees_human_rights',
    'controv_employees_human_rights_sc', 'controv_energy_use_and_ghg_emissions',
    'controv_energy_use_and_ghg_emissions_sc', 'controv_environmental_impact_of_products',
    'controv_intellectual_property', 'controv_labour_relations', 'controv_labour_relations_sc',
    'controv_land_use_and_biodiversity', 'controv_land_use_and_biodiversity_sc',
    'controv_lobbying_and_public_policy', 'controv_marketing_practices', 'controv_media_ethics',
    'controv_occupational_health_and_safety', 'controv_occupational_health_and_safety_sc',
    'controv_quality_and_safety', 'controv_sanctions', 'controv_social_impact_of_products',
    'controv_society_human_rights', 'controv_society_human_rights_sc', 'controv_water_use',
    'controv_water_use_sc', 'controv_weapons'
]

# Select Relevant Datafeed's Fields
controv_1 = df_1_filtered.loc[:, controversies_column_list]
controv_2 = df_2_filtered.loc[:, controversies_column_list]

# Merge Dataframes from both Months
controv_df = pd.merge(controv_1, controv_2, on='permid', suffixes=('_before', '_after'))
controv_df.drop('issuer_name_after', axis=1, inplace=True)
controv_df.rename(columns={'issuer_name_before': 'issuer_name'}, inplace=True)

# Fill NaN values with 0
columns_to_fill = controv_df.columns.difference(['issuer_name', 'permid'])
controv_df[columns_to_fill] = controv_df[columns_to_fill].fillna(0)

# Order Columns
controv_df = sort_col_suffix(controv_df)

# Create Variation Columns
controv_df = add_variation_columns(controv_df)

# Create overall variation column
variation_cols_contr = [col for col in controv_df.columns if col.startswith("variation_")]
controv_df["controv_variation"] = controv_df[variation_cols_contr].any(axis=1)

# Reorder columns
column_order = ['permid', 'issuer_name', 'controv_variation', 'any_critical_controversy_after']
variation_columns = [col for col in controv_df.columns if col.startswith('variation_')]
other_columns = sorted(set(controv_df.columns) - set(column_order) - set(variation_columns))
controv_df = controv_df[column_order + variation_columns + other_columns]

# Select the Columns with the variation that will be added to the final Dataframe
controversies_variation_true = controv_df[controv_df['controv_variation'] == True]
controversias = controversies_variation_true[['permid', 'issuer_name', 'controv_variation']]

# Merge those equities with variations with their strategies
controversies_variation_str001 = pd.merge(controversies_variation_true, portfolio_str001, on='permid')
controversies_variation_str002 = pd.merge(controversies_variation_true, portfolio_str002, on='permid')
controversies_variation_str003 = pd.merge(controversies_variation_true, portfolio_str003, on='permid')
controversies_variation_str003b = pd.merge(controversies_variation_true, portfolio_str003b, on='permid')
controversies_variation_str004 = pd.merge(controversies_variation_true, portfolio_str004, on='permid')
controversies_variation_str005 = pd.merge(controversies_variation_true, portfolio_str005, on='permid')
controversies_variation_art8 = pd.merge(controversies_variation_true, portfolio_art8, on='permid')
controversies_variation_cs001 = pd.merge(controversies_variation_true, portfolio_cs001, on='permid')

logging.info("Controversies processing completed")

# Strategies
logging.info("Processing strategies")

logging.info("Processing strategies")

try:
    # Select Relevant Datafeed's Fields
    strategies_columns = ['issuer_name', 'permid', 'str_001_s', 'str_002_ec', 'str_003_ec', 'str_004_asec', 
                          'str_005_ec', 'cs_001_sec', 'cs_003_sec', 'cs_002_ec', 'str_006_sec', 'str_007_sect', 
                          'art_8_basicos', 'str_003b_ec', 'gp_esccp_22']

    strategies_1 = df_1_filtered.loc[:, strategies_columns].copy()
    strategies_2 = df_2_filtered.loc[:, strategies_columns].copy()

    # Ensure 'permid' is unique in both dataframes
    strategies_1 = strategies_1.drop_duplicates(subset=['permid'])
    strategies_2 = strategies_2.drop_duplicates(subset=['permid'])

    # Merge Dataframes from both Months
    strategies_df = pd.merge(strategies_1, strategies_2, on='permid', suffixes=('_before', '_after'))
    strategies_df['issuer_name'] = strategies_df['issuer_name_before'].fillna(strategies_df['issuer_name_after'])
    strategies_df.drop(['issuer_name_before', 'issuer_name_after'], axis='columns', inplace=True)

    # Fill NaN values with 0
    columns_to_fill = strategies_df.columns.difference(['permid', 'issuer_name'])
    strategies_df[columns_to_fill] = strategies_df[columns_to_fill].fillna(0)

    # Reset index to avoid any potential issues with duplicate indices
    strategies_df.reset_index(drop=True, inplace=True)

    # Order Columns
    strategies_df = sort_col_suffix(strategies_df)

    # Create Variation Columns
    strategies_df = add_variation_columns(strategies_df)

    variation_cols_estrat = [col for col in strategies_df.columns if col.startswith("variation_")]

    # Create overall variation column
    strategies_df["variation_estrategias"] = strategies_df[variation_cols_estrat].any(axis=1)

    # Reorder columns
    column_order = ['permid', 'issuer_name', 'variation_estrategias']
    variation_columns = [col for col in strategies_df.columns if col.startswith('variation_')]
    other_columns = sorted(set(strategies_df.columns) - set(column_order) - set(variation_columns))
    strategies_df = strategies_df[column_order + variation_columns + other_columns]

    # Print debug information
    logging.info(f"strategies_df shape: {strategies_df.shape}")
    logging.info(f"strategies_df dtypes:\n{strategies_df.dtypes}")
    logging.info(f"'variation_estrategias' nunique values: {strategies_df['variation_estrategias'].nunique()}")
    
    # Check if 'variation_estrategias' is a single column or multiple columns
    if isinstance(strategies_df['variation_estrategias'], pd.DataFrame):
        logging.info("'variation_estrategias' is a DataFrame, not a Series. Columns:")
        logging.info(strategies_df['variation_estrategias'].columns)
        # Select only the first column if it's a DataFrame
        strategies_df['variation_estrategias'] = strategies_df['variation_estrategias'].iloc[:, 0]
    
    logging.info(f"'variation_estrategias' value counts:\n{strategies_df['variation_estrategias'].value_counts()}")

    # Ensure 'variation_estrategias' is boolean
    strategies_df['variation_estrategias'] = strategies_df['variation_estrategias'].astype(bool)

    # Filter rows where variation_estrategias is True
    estrategias_true = strategies_df[strategies_df['variation_estrategias']].copy()

    logging.info(f"Number of rows in estrategias_true: {len(estrategias_true)}")

    # Select the Columns with the variation that will be added to the final Dataframe
    estrategias = strategies_df[['permid', 'issuer_name', 'variation_estrategias']]

    # Merge with portfolio strategies
    estrategias_true_str001 = pd.merge(estrategias_true, portfolio_str001, on='permid', how='inner')
    estrategias_true_str002 = pd.merge(estrategias_true, portfolio_str002, on='permid', how='inner')
    estrategias_true_str003 = pd.merge(estrategias_true, portfolio_str003, on='permid', how='inner')
    estrategias_true_str003b = pd.merge(estrategias_true, portfolio_str003b, on='permid', how='inner')
    estrategias_true_str004 = pd.merge(estrategias_true, portfolio_str004, on='permid', how='inner')
    estrategias_true_str005 = pd.merge(estrategias_true, portfolio_str005, on='permid', how='inner')
    estrategias_true_art8 = pd.merge(estrategias_true, portfolio_art8, on='permid', how='inner')
    estrategias_true_cs001 = pd.merge(estrategias_true, portfolio_cs001, on='permid', how='inner')

    logging.info(f"Strategies processing completed successfully. Total variations: {len(estrategias_true)}")

except ValueError as ve:
    logging.error(f"ValueError in strategies processing: {str(ve)}")
    raise
except Exception as e:
    logging.error(f"Error in strategies processing: {str(e)}")
    raise

# Log some information about the resulting dataframes
logging.info(f"Estrategias shape: {estrategias.shape}")
logging.info(f"Estrategias_true shape: {estrategias_true.shape}")
for strat in ['str001', 'str002', 'str003', 'str003b', 'str004', 'str005', 'art8', 'cs001']:
    df = locals()[f'estrategias_true_{strat}']
    logging.info(f"Estrategias_true_{strat} shape: {df.shape}")
# Gather and export variations
logging.info("Gathering and exporting variations")

# Define the DataFrames to be merged along with their keys
dataframes = [
    (sustainability_rating[['permid', 'sust_rating_variation']], 'permid'),
    (controversias[['permid', 'controv_variation']], 'permid'),
    (estrategias[['permid', 'variation_estrategias']], 'permid')
]

# Start with the exposures DataFrame
data = pd.concat([exposures] + [df.set_index(key) for df, key in dataframes], axis=1)
data = data.reset_index()

# Rename columns for consistency
data.rename(columns={
    'max_expo_variation': 'variation_max_expo',
    'controv_variation': 'variation_controv'
}, inplace=True)

# Create an overall variation column
variation_df_cols = [col for col in data.columns if col.startswith("variation_")]
data["variation_df"] = data[variation_df_cols].any(axis=1)

# Merge data with portfolio strategies
data_str001 = pd.merge(data, portfolio_str001, on='permid')
data_str002 = pd.merge(data, portfolio_str002, on='permid')
data_str003 = pd.merge(data, portfolio_str003, on='permid')
data_str003b = pd.merge(data, portfolio_str003b, on='permid')
data_str004 = pd.merge(data, portfolio_str004, on='permid')
data_str005 = pd.merge(data, portfolio_str005, on='permid')
data_art8 = pd.merge(data, portfolio_art8, on='permid')
data_cs001 = pd.merge(data, portfolio_cs001, on='permid')

# Prepare data for export
export_data = {
    'STR001.xlsx': [data_str001, sust_rat_variation_true_str001, exposure_variation_true_str001, controversies_variation_str001, estrategias_true_str001],
    'STR002.xlsx': [data_str002, sust_rat_variation_true_str002, exposure_variation_true_str002, controversies_variation_str002, estrategias_true_str002],
    'STR003.xlsx': [data_str003, sust_rat_variation_true_str003, exposure_variation_true_str003, controversies_variation_str003, estrategias_true_str003],
    'STR003B.xlsx': [data_str003b, sust_rat_variation_true_str003b, exposure_variation_true_str003b, controversies_variation_str003b, estrategias_true_str003b],
    'STR004.xlsx': [data_str004, sust_rat_variation_true_str004, exposure_variation_true_str004, controversies_variation_str004, estrategias_true_str004],
    'STR005.xlsx': [data_str005, sust_rat_variation_true_str005, exposure_variation_true_str005, controversies_variation_str005, estrategias_true_str005],
    'ART8.xlsx': [data_art8, sust_rat_variation_true_art8, exposure_variation_true_art8, controversies_variation_art8, estrategias_true_art8],
    'CS001.xlsx': [data_cs001, sust_rat_variation_true_cs001, exposure_variation_true_cs001, controversies_variation_cs001, estrategias_true_cs001]
}

sheet_names = ['Resumen', 'Sust_R', 'Max_Exp', 'Controversias', 'Estrategias']

# Create output directory
base_dir = rf"C:\Users\n740789\Documents\Projects_local\DataSets\overrides\analisis_cambios_ovr\{DATE}\\"
create_output_directory(base_dir)

# Export results
for file_name, sheets in export_data.items():
    full_path = os.path.join(base_dir, file_name)
    with pd.ExcelWriter(full_path, engine='openpyxl') as writer:
        for sheet_name, df in zip(sheet_names, sheets):
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    logging.info(f"Exported {file_name}")

logging.info("All data exported successfully")

# Create output directory
base_dir = rf"C:\Users\n740789\Documents\Projects_local\DataSets\overrides\analisis_cambios_ovr\{DATE}\\"
create_output_directory(base_dir)

# Export results
# We are going to change the name of the files to conduct some analysis and add new features. 2024/09/03.
logging.info("Exporting results")
paths = {
    'STR001_v2.xlsx': [hoja_str001_1, hoja_str001_2, hoja_str001_3, hoja_str001_4, hoja_str001_5],
    'STR002_v2.xlsx': [hoja_str002_1, hoja_str002_2, hoja_str002_3, hoja_str002_4, hoja_str002_5],
    'STR003_v2.xlsx': [hoja_str003_1, hoja_str003_2, hoja_str003_3, hoja_str003_4, hoja_str003_5],
    'STR003B_v2.xlsx': [hoja_str003b_1, hoja_str003b_2, hoja_str003b_3, hoja_str003b_4, hoja_str003b_5],
    'STR004_v2.xlsx': [hoja_str004_1, hoja_str004_2, hoja_str004_3, hoja_str004_4, hoja_str004_5],
    'STR005_v2.xlsx': [hoja_str005_1, hoja_str005_2, hoja_str005_3, hoja_str005_4, hoja_str005_5],
    'ART8_v2.xlsx': [hoja_art8_1, hoja_art8_2, hoja_art8_3, hoja_art8_4, hoja_art8_5],
    'CS001_v2.xlsx': [hoja_cs001_1, hoja_cs001_2, hoja_cs001_3, hoja_cs001_4, hoja_cs001_5]
}

sheet_names = ['Resumen', 'Sust_R', 'Max_Exp', 'Controversias', 'Estrategias']

for file_name, sheets in paths.items():
    full_path = base_dir + file_name
    with pd.ExcelWriter(full_path) as writer:
        for sheet, data in zip(sheet_names, sheets):
            data.to_excel(writer, sheet_name=sheet, index=False)
    logging.info(f"Exported {file_name}")

# Calculate and print execution time
end = time.time()
execution_time = end - start
logging.info(f"Script execution completed in {execution_time:.2f} seconds")
print(f"Script execution completed in {execution_time:.2f} seconds")