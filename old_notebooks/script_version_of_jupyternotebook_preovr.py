# %% [markdown]
# # CLARITY AI DATAFEED VARIATIONS  
# 
# ### Goal: Find out Monthly Variation on Sustainability Ratings, Exposure, Controversies, and Strategies previous to the overwrite of the data.
# 
# ### Steps
#  1. **Libraries and Datasets:**   
#     **1.1. Import**  
#     **1.2. Apply Transformations on Datasets**
# 
# 
#  2. **Define the Portfilio/Carteras & Strategies Lists**
#     **2.1. Read Portfolio List**    
#     **2.2. Crossreference Porfolio & Filter**    
#     **2.3. Define Strategies Dataframes**   
# 
# 
#  3. **Find Variations on:**  
#     **3.1. Sustainability Ratings**  
#     **3.2. Exposure**  
#     **3.3. Controversies**  
#     **3.4. Strategies**  
# 
#    For each one of these 4 dimensions variations we will follow the next substeps:  
#          *a. Select Relevant Datafeed’s Fields.*    
#          *b.	Merge Dataframes from both Months.*    
#          *c.	Order Columns.*    
#          *d.	Create Variation Columns.*  
#          *e.	Select the Columns with the variation that will be add to the final Dataframe.*  
#          *f.	Merge those equities with variations with their strategies.*  
# 
#       
# 
#  4. **Gather and Export Variations.**
#  5. **Apply Overwrites.** 
#     

# %% [markdown]
# ### 1. Libraries and Datafeeds
# #### 1.1. Import & Read

# %%
#Import libraries
import numpy as np
import pandas as pd
import os
import time
import re


# %%
#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_columns', None)

# %%
#Start timer to check script performance
start = time.time()

# %%
DATE_PREV = "202409"
DATE = "202410"

# %% [markdown]
# #### The dataset we will need are:
# - Clarity's Equities Feed already without duplicates
# - Cross Reference of Portfolio between Aladdin and Clarity AI
# - Out portfolios, a.k.a. carteras
# - Our Benchmark, i.e. the spcace of optionas from where portfolios can be composed.

# %% [markdown]
# Clarity's Datafeed Cleaned of Duplicates 

# %%

df_1_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE_PREV}01_Equities_feed_IssuerLevel_sinOVR.csv"
df_2_path = rf"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\ficheros_tratados\{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv"

df_1=pd.read_csv(df_1_path, sep=',', dtype='unicode')
df_2=pd.read_csv(df_2_path, sep=',', dtype='unicode')

# %% [markdown]
# Cross References

# %%
# Cross reference column names [aladdin_id, Issuer_Name, CLARITY_AI, MSCI, SUST]
xref_1 =pd.read_csv(rf'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_{DATE_PREV}01.csv', dtype={'CLARITY_AI': str})
xref_2 =pd.read_csv(rf'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_{DATE}01.csv', dtype={'CLARITY_AI': str})

# %% [markdown]
# Portfolios/Carteras

# %%
# carteras column names [Issuer Name, aladdin_id, Security Description, Portfolio Full Name, portfolio_id]

car_1 = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\aladdin_carteras_benchmarks\{DATE_PREV}01_snt world_portf_bmks.xlsx', sheet_name = "portfolio_carteras", skiprows=3)
car_2 = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\aladdin_carteras_benchmarks\{DATE}01_snt world_portf_bmks.xlsx', sheet_name = "portfolio_carteras", skiprows=3)

# %% [markdown]
# Benchmarks

# %%
# benchmark column names [Issuer Name, aladdin_id, Portfolio Full Name, benchmark_id, ESG Score(Wt Avg-BENCH NMV/MV)]
#bmk_1 = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\aladdin_carteras_benchmarks\carteras_benchmark_{DATE_PREV}.xlsx', sheet_name ="portfolio_benchmarks", skiprows=3)
#bmk_2 = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\aladdin_carteras_benchmarks\carteras_benchmark_{DATE}.xlsx', sheet_name ="portfolio_benchmarks", skiprows=3)

# %% [markdown]
# ### 1.2. Transform & Prep Datasets for Analysis
# 
# Let's Start transforming and adapting these datasets.

# %%
# Change Column Names
list_dfs = [car_1, car_2, xref_1, xref_2]

for df in list_dfs:
    df.columns = df.columns.str.lower().str.strip().str.replace(" ","_")


# %%
df_1.columns = df_1.columns.str.lower().str.strip().str.replace(" ","_")
df_2.columns = df_2.columns.str.lower().str.strip().str.replace(" ","_")

# %%

# Transform the df & name the colums
car_1 = car_1.loc[:,["aladdin_id", 'portfolio_id', 'security_description']] # aladdin_id used to be named issuer_id and portfolio_name is now portfolio_id

car_2 = car_2.loc[:,["aladdin_id", 'portfolio_id', 'security_description']]

# Rename Cross Reference df. 

xref_1.rename(columns={'clarity_ai':'permid', 'aladdin_issuer': 'aladdin_id'}, inplace=True)

xref_2.rename(columns={'clarity_ai':'permid', 'aladdin_issuer': 'aladdin_id'}, inplace=True)


xref_1 = xref_1.loc[:,['aladdin_id', 'issuer_name', 'permid']]
xref_2 = xref_2.loc[:,['aladdin_id', 'issuer_name', 'permid']]

# Drop from Carteras Rows with empty aladdin_id
car_1 = car_1.dropna(subset=['aladdin_id'])
car_2 = car_2.dropna(subset=['aladdin_id'])


# %% [markdown]
# ### 2. Carteras / Portfolios & Strategies Lists
# 
# 2.1. Read Portfolio List  
# 2.2. Crossreference Porfolio & Filter  
# 2.3. Define Strategies Dataframes  
# 
# 

# %% [markdown]
# #### 2.1. Read Portfolio Lists
# 
# We have created a text file document, carteras_list.txt, where we record the list of portfolio we want to filter by.
# 
# This list has to be updated manually any time there is a new portfolio to monintor (ASG - SOST - ART8 BAS).
# 
# The list of portfolioas are those of the following strategies:
# 
# - STR001		
# - STR002		
# - STR003		
# - STR004		
# - STR004B	
# - STR005		
# - STR006	
# - STR007	
# - CS001	
# - ART8 

# %%
# We open, read, and create a list with the portfolio we want to use.
## Create a sheet with fields for each strategy. TBD 
with open(r'C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\carteras_list.txt', 'r') as f:
    carteras_list = [line.strip() for line in f.readlines()]

# Filtrar por la lista de nombres
car_1_strat = car_1[car_1['portfolio_id'].isin(carteras_list)].copy()

car_2_strat = car_2[car_2['portfolio_id'].isin(carteras_list)].copy()

# %% [markdown]
# #### 2.2. Cross Reference & Filters.
# 
# First, Filter Cross Reference by the Portfolio/Cartera issuer id.  
# 
# Filtering based on match that of cross reference' aladdin_id with cartera's issuer_i.
# 
# Second, we will used these filtered cross reference to then filter the main datafeed. 

# %%

# First, filter cross reference to match portfolios in carteras
xref_1_filtered = xref_1.loc[xref_1['aladdin_id'].isin(car_1_strat['aladdin_id'])]
xref_1_filtered = xref_1_filtered[xref_1_filtered['permid'].notna()] # drop empty ones.

xref_2_filtered = xref_2.loc[xref_2['aladdin_id'].isin(car_2_strat['aladdin_id'])]
xref_2_filtered = xref_2_filtered[xref_2_filtered['permid'].notna()]# drop empty ones.

# Second, used the filtered cross df to filter the main datafeed
df_1_filtered = df_1.loc[df_1['permid'].isin(xref_1_filtered['permid'])]
df_2_filtered = df_2.loc[df_2['permid'].isin(xref_2_filtered['permid'])]

# %% [markdown]
# #### 2.3. Strategies 
# ###### This step is to be updated and on the future we will read from a file.
# Whenever there is a new portfolio, the list below will need to be updated.

# %%
# Asigna las carteras a una lista específica por estrategia. Mirar documentos 06024_Estrategias_Mandatos_ISR y Abril_2024_Inventario reglas Compliance
lista_str001 = ["FPG00002","FPG01072","EPV00005","EPV14005", "FPG01079","FPG01697","FPG00112","EPV00043","EPV14004","LXMS0720", "FPG00028", "FPG01214", "FPG00027", "FPG00603", "FPG01213", "EPV00060", "EPV00061", "EPV00062", "EPV00063", "EPV00064"]
lista_str004 = ["FPH00457", "EPH00107", "FIG05273", "FPB01158", "FIG05240", "FIG05241", "FIG05402", "FIG00677", "PFI01541"]
lista_art8 = ["FPG00015","FPG01421","FPH00254","EPV00052","EPV00051","EPV00050","FPG00026","FPG00555","EPV14006","FIF00154","FIF04284","FIF04285","FIG00026","FIG00689","FIG01973","FIG01988","FIG01998","FIG02164","FIG02410","FIG04251","FIG04253","FIG04868","FIG05178","FIG05198","FIG05415","FIG05428","FIG05698","FIH00058","FIH00208","FIH01197","FIH01920","FIN00315","FIN00441","FIN00637","CMD07211","CMD07212","CMD07213","CMD07214","CMD07215","CMD08211","CMD08212","CMD08213","CMD08214","CMD08215","PFIT0042","PFIT0516","PFIT0605","PFIT0647","PFIT1421","PFIT1422","PFIT1423","PFIT1424","PFIT1425","PFIT1426","PFIT1830","PFS00001","PFS00002","PFS00058","PFS00059","PFS00358","PFS00359","PFS00360","PFS00365","PFS00366","PFS00371","PFSULM01","PFSULM02","PFSULM03","PFSULM04","PFSULM05","LXMS0020","LXMS0040","LXMS0240","LXMS0360","LXMS0520","LXMS0530","LXMS0540","LXMS0340","LXMS0350","LXMS0650","LXMS0630","LXMS0550","LXMS0560","LXMS0570","PFI01549","PFIT0011","FPG00687","FPG01066","LXMS0580","FIG05774","LXMS0730","PFI01546","LXMS0750","PFPP0BST","PFP00BCD","PFP00BBD","LXMS0770","FPG00776","FPH00630","FPH00704","FPG00017","FPG00376","FPG00020","FPG00021","FPG00301","FPG00004","FPG00008","FPG01440","FPG20002","FPG01212","FPH00012","FPG01873","FPG00005","FPG16000","FPG01282","FPG15900","FPG00688","FPG01082","FPG00010","FPG01119","FPH00706","FPG00233","FPG01161","FPH00446","FPG00024","FPG00006","FPG00018","FPG01170","FPG00011","FPG01758"]
lista_str002 = ["FIG02787","CPE03861"]
lista_str005 = ["FIH00529","CPE04024"]
lista_str003 = ["FPG01278"]
lista_str003b = ["CPE00035","CPE00169","CPE00264","CPE00277","CPE00363","CPE00375","CPE00448","CPE00456","CPE00464","CPE03002","CPE03290","CPE03519","CPE03570","CPE03853","CPE03867","CPE03912","CPE03913","CPE03928","CPE03986","CPE04036","CPE04037","CPE04039","CPE04041","CPE04044","CPE04053","CPE04059","CPE04063","CPE04070","CPE04071","CPE04075","CPE04076","CPE04077","CPE04080","CPE04081","CPE04082","CPE04084","CPE04087","CPE05134","CPE05137","CPE05148","CPE05150","CPE05151","CPE05152","CPE05162","CPE05174","CPE05177","CPE05180","CPE05189","CPE05190","CPE05191","CPE05192","CPE05207","CPE05208","CPE05211","CPE05212","CPE05218","CPE05219","CPE05226","CPE05234","CPE05239","CPE05242","CPE05244","CPE05245","CPE05248","CPE05252","CPE05258","CPE05261","CPE05314","CPE05315","CPE05317","CPE05318","CPE05337","CPE05340","CPE05341","CPE05344","CPE05351","CPE05355","CPE05361","CPE05362","CPE05363","CPE05366"]
lista_cs001 = ["CPE03744"]

#usa la lista para crear vectores de carteras con sólo los nombres dentro de la lista creada con los PF correspondientes a cada STR
portfolio_str001 = car_2[car_2['portfolio_id'].isin(lista_str001)]
portfolio_str002 = car_2[car_2['portfolio_id'].isin(lista_str002)]
portfolio_str003 = car_2[car_2['portfolio_id'].isin(lista_str003)]
portfolio_str003b = car_2[car_2['portfolio_id'].isin(lista_str003b)]
portfolio_str004 = car_2[car_2['portfolio_id'].isin(lista_str004)]
portfolio_str005 = car_2[car_2['portfolio_id'].isin(lista_str005)]
portfolio_art8 = car_2[car_2['portfolio_id'].isin(lista_art8)]
portfolio_cs001 = car_2[car_2['portfolio_id'].isin(lista_cs001)]

#Merge with XREF
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

# %% [markdown]
# We will use this list to merge with the variations on Clarity's Datafeed that we find in the next steps.

# %% [markdown]
# Export Results

# %%
#Chequeo de los resultados en los DF antes y dspués filtering
#DF_before_filtered.to_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/Análisis_Cambios_OVR/2024_04/DF_before_filtered.xlsx', sheet_name='Resumen', index=False)
#DF_after_filtered.to_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/Análisis_Cambios_OVR/2024_04/DF_after_filtered.xlsx', sheet_name='Resumen', index=False)

# %% [markdown]
# ### 3. Find Monthly Variations
# 
# For each dimension (Sustainabili Ratings, Exposure, Controversies, and Strategies) we will follow the next steps:
# 
# 
# a.	Select Relevant Datafeed’s Fields.  
# b.	Merge Dataframes from both Months.  
# c.	Order Columns.  
# d.	Create Variation Columns.  
# e.	Select the Columns with the variation that will be add to the final Dataframe.  
# f.	Merge those equities with variations with their strategies.  
# 
# But before we will define a couple of functions that will help us with step c & e.

# %%
# Define Function for step c, sort/order columns.
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


# previous variation column maker 
def add_variation_columns(df):
    cols = df.filter(regex='_before$').columns
    def generate_variation_col(col):
        col_name = col.rpartition("_")[0]
        before_col = col.replace("_before", "_after")
        verification_col = "variation_" + col_name
        bool_series = df[col].ne(df[before_col])
        return verification_col, bool_series
    for col in cols:
        verification_col, bool_series = generate_variation_col(col)
        df.insert(df.columns.get_loc(col), verification_col, bool_series)
    return df

# %% [markdown]
# #### 3.1. Sustainability_rating

# %%
# a.    Select Relevant Fileds/Columns
df_1_sutainability_ratings = df_1_filtered.loc[:, ['issuer_name', 'permid', 'sustainability_rating']]
df_2_sutainability_ratings = df_2_filtered.loc[:, ['issuer_name', 'permid', 'sustainability_rating']]

# b.    Merge df of both months on permid
sutainability_ratings_df = pd.merge(df_1_sutainability_ratings, df_2_sutainability_ratings, on='permid',suffixes=('_before', '_after'))
sutainability_ratings_df.drop(['issuer_name_after'], axis = 'columns', inplace=True)
sutainability_ratings_df.rename(columns = {'issuer_name_before':'issuer_name'},inplace =True)
sutainability_ratings_df = sutainability_ratings_df[['permid','issuer_name', 'sustainability_rating_before','sustainability_rating_after']] 

# Since in Sustainability rating there is only one columnn, we will skip step c.

# d.    Create Variation Column
sutainability_ratings_df['sust_rating_variation'] = sutainability_ratings_df.apply(
    lambda row: row['sustainability_rating_before'] != row['sustainability_rating_after'], axis=1)

# e.    Select the variation column for the final dataframe
sustainability_rating = sutainability_ratings_df[['permid','issuer_name','sust_rating_variation']]

# f.    Merge those equities with variations with their strategies.

#As part of setp f: Create a df with the portfolios that changed
sust_rat_variation_true = sutainability_ratings_df[sutainability_ratings_df['sust_rating_variation']==True]
#As part of setp  f: Define the order of those columns
sust_rat_variation_true = sust_rat_variation_true[['permid','issuer_name','sust_rating_variation','sustainability_rating_before','sustainability_rating_after']]

sust_rat_variation_true_str001 = pd.merge(sust_rat_variation_true, portfolio_str001, on='permid')
sust_rat_variation_true_str002 = pd.merge(sust_rat_variation_true, portfolio_str002, on='permid')
sust_rat_variation_true_str003 = pd.merge(sust_rat_variation_true, portfolio_str003, on='permid')
sust_rat_variation_true_str003b = pd.merge(sust_rat_variation_true, portfolio_str003b, on='permid')
sust_rat_variation_true_str004 = pd.merge(sust_rat_variation_true, portfolio_str004, on='permid')
sust_rat_variation_true_str005 = pd.merge(sust_rat_variation_true, portfolio_str005, on='permid')
sust_rat_variation_true_art8 = pd.merge(sust_rat_variation_true, portfolio_art8, on='permid')
sust_rat_variation_true_cs001 = pd.merge(sust_rat_variation_true, portfolio_cs001, on='permid')

# %%
# Export results, if necessary
#sust_rat_variation_true.to_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/Misbel/Pruebas/Var_sust_rat_variation_true.xlsx',index=False)

# %% [markdown]
# #### 3.2. Exposure

# %%
# a.    Selec Exposure Relevant Fields
exposure_column_list =  ['issuer_name','permid','maxexp_abortifacents_part','maxexp_alcohol_part','maxexp_alchol_prod','maxexp_antip_landmines_part',
'maxexp_armament_part','maxexp_armament_prod','maxexp_chemic_biolog_weapons_part','maxexp_cluster_bombs_part','maxexp_coal_mining_part',
'maxexp_coal_mining_prod','maxexp_coal_power_gen_part','maxexp_coal_power_gen_prod','maxexp_contraceptives_part','maxexp_embryonic_stem_cell_research_part',
'maxexp_fossil_fuels_part','maxexp_fossil_fuels_prod','maxexp_gambling_part', 'maxexp_gambling_prod','maxexp_gmo_products_part','maxexp_gmo_research_part',
'maxexp_nuclear_weapons_part','maxexp_oil_sands_part','maxexp_oil_sands_prod','maxexp_palm_oil_part','maxexp_pornography_part','maxexp_shale_energy_part',
'maxexp_shale_energy_prod','maxexp_stem_cell_research','maxexp_tobacco_part','maxexp_tobacco_prod','maxexp_artic_oil_part','maxexp_small_arms_part',
'maxexp_white_phosporus_part','maxexp_nuclear_weapons_prod','maxexp_cluster_bombs_prod','maxexp_thermal_coal_prod','maxexp_gas_fuels_prod','maxexp_gas_fuels_part',
'maxexp_oil_fuels_prod','maxexp_oil_fuels_part']

# Filter to create exposure dfs
exposure_1 = df_1_filtered.loc[:, exposure_column_list]
exposure_2 = df_2_filtered.loc[:, exposure_column_list]

# b.    Merge these expose dfs en función de permid
exposure_df = pd.merge(exposure_1, exposure_2, on='permid', suffixes=('_before', '_after'))

# We will apply some transformations to the new merged df
exposure_df.drop(['issuer_name_after'], axis = 'columns', inplace=True)
exposure_df.rename(columns = {'issuer_name_before':'issuer_name'},inplace =True)

# We will fill empty valalues with "0"
exposure_df = exposure_df.fillna(0)

# %%
# c.    Order Columns
exposure_df = sort_col_suffix(exposure_df)

# %%
# Select columns/fields relevant to exposure
exposure_df = exposure_df[['permid',
 'issuer_name',
 'maxexp_abortifacents_part_before',
 'maxexp_abortifacents_part_after',
 'maxexp_alchol_prod_before',
 'maxexp_alchol_prod_after',
 'maxexp_alcohol_part_before',
 'maxexp_alcohol_part_after',
 'maxexp_antip_landmines_part_before',
 'maxexp_antip_landmines_part_after',
 'maxexp_armament_part_before',
 'maxexp_armament_part_after',
 'maxexp_armament_prod_before',
 'maxexp_armament_prod_after',
 'maxexp_artic_oil_part_before',
 'maxexp_artic_oil_part_after',
 'maxexp_chemic_biolog_weapons_part_before',
 'maxexp_chemic_biolog_weapons_part_after',
 'maxexp_cluster_bombs_part_before',
 'maxexp_cluster_bombs_part_after',
 'maxexp_coal_mining_part_before',
 'maxexp_coal_mining_part_after',
 'maxexp_coal_mining_prod_before',
 'maxexp_coal_mining_prod_after',
 'maxexp_coal_power_gen_part_before',
 'maxexp_coal_power_gen_part_after',
 'maxexp_coal_power_gen_prod_before',
 'maxexp_coal_power_gen_prod_after',
 'maxexp_contraceptives_part_before',
 'maxexp_contraceptives_part_after',
 'maxexp_embryonic_stem_cell_research_part_before',
 'maxexp_embryonic_stem_cell_research_part_after',
 'maxexp_fossil_fuels_part_before',
 'maxexp_fossil_fuels_part_after',
 'maxexp_fossil_fuels_prod_before',
 'maxexp_fossil_fuels_prod_after',
 'maxexp_gambling_part_before',
 'maxexp_gambling_part_after',
 'maxexp_gambling_prod_before',
 'maxexp_gambling_prod_after',
 'maxexp_gmo_products_part_before',
 'maxexp_gmo_products_part_after',
 'maxexp_gmo_research_part_before',
 'maxexp_gmo_research_part_after',
 'maxexp_nuclear_weapons_part_before',
 'maxexp_nuclear_weapons_part_after',
 'maxexp_oil_sands_part_before',
 'maxexp_oil_sands_part_after',
 'maxexp_oil_sands_prod_before',
 'maxexp_oil_sands_prod_after',
 'maxexp_palm_oil_part_before',
 'maxexp_palm_oil_part_after',
 'maxexp_pornography_part_before',
 'maxexp_pornography_part_after',
 'maxexp_shale_energy_part_before',
 'maxexp_shale_energy_part_after',
 'maxexp_shale_energy_prod_before',
 'maxexp_shale_energy_prod_after',
 'maxexp_small_arms_part_before',
 'maxexp_small_arms_part_after',
 'maxexp_stem_cell_research_before',
 'maxexp_stem_cell_research_after',
 'maxexp_tobacco_part_before',
 'maxexp_tobacco_part_after',
 'maxexp_tobacco_prod_before',
 'maxexp_tobacco_prod_after',
 'maxexp_white_phosporus_part_before',
 'maxexp_white_phosporus_part_after',
 'maxexp_nuclear_weapons_prod_before',
 'maxexp_nuclear_weapons_prod_after',
 'maxexp_cluster_bombs_prod_before',
 'maxexp_cluster_bombs_prod_after',
 'maxexp_thermal_coal_prod_before',
 'maxexp_thermal_coal_prod_after',
 'maxexp_gas_fuels_prod_before',
 'maxexp_gas_fuels_prod_after',                     
 'maxexp_gas_fuels_part_before',
 'maxexp_gas_fuels_part_after',                     
 'maxexp_oil_fuels_prod_before',
 'maxexp_oil_fuels_prod_after',
 'maxexp_oil_fuels_part_before',
 'maxexp_oil_fuels_part_after']]
 

# %%
# Transform certain values to integers
exposure_df.iloc[:,2:] = exposure_df.iloc[:,2:].astype('float').astype('int')

# %%
# d.    Create a Variation Columnm.
exposure_df = add_variation_columns(exposure_df)

variation_cols = [col for col in exposure_df.columns if col.startswith("variation_")]

exposure_df["max_expo_variation"] = exposure_df[variation_cols].any(axis=1)

# %%
#Obtener resultado ordenado, vista de variaciones como principios
column_order_e = ['permid','issuer_name', 'max_expo_variation']
variation_columns_e = [col for col in exposure_df.columns if col.startswith('variation_')]
other_columns_e = sorted(set(exposure_df.columns)- set(column_order_e) - set(variation_columns_e))
exposure_df = exposure_df[column_order_e + variation_columns_e + other_columns_e]

exposure_df = exposure_df[['permid',
 'issuer_name',
 'max_expo_variation',
 'variation_maxexp_abortifacents_part',
 'variation_maxexp_alchol_prod',
 'variation_maxexp_alcohol_part',
 'variation_maxexp_antip_landmines_part',
 'variation_maxexp_armament_part',
 'variation_maxexp_armament_prod',
 'variation_maxexp_artic_oil_part',
 'variation_maxexp_chemic_biolog_weapons_part',
 'variation_maxexp_cluster_bombs_part',
 'variation_maxexp_coal_mining_part',
 'variation_maxexp_coal_mining_prod',
 'variation_maxexp_coal_power_gen_part',
 'variation_maxexp_coal_power_gen_prod',
 'variation_maxexp_contraceptives_part',
 'variation_maxexp_embryonic_stem_cell_research_part',
 'variation_maxexp_fossil_fuels_part',
 'variation_maxexp_fossil_fuels_prod',
 'variation_maxexp_gambling_part',
 'variation_maxexp_gambling_prod',
 'variation_maxexp_gmo_products_part',
 'variation_maxexp_gmo_research_part',
 'variation_maxexp_nuclear_weapons_part',
 'variation_maxexp_oil_sands_part',
 'variation_maxexp_oil_sands_prod',
 'variation_maxexp_palm_oil_part',
 'variation_maxexp_pornography_part',
 'variation_maxexp_shale_energy_part',
 'variation_maxexp_shale_energy_prod',
 'variation_maxexp_small_arms_part',
 'variation_maxexp_stem_cell_research',
 'variation_maxexp_tobacco_part',
 'variation_maxexp_tobacco_prod',
 'variation_maxexp_white_phosporus_part',
 'variation_maxexp_nuclear_weapons_prod',
 'variation_maxexp_cluster_bombs_prod',
 'variation_maxexp_thermal_coal_prod',
 'variation_maxexp_gas_fuels_prod',
 'variation_maxexp_gas_fuels_part',
 'variation_maxexp_oil_fuels_prod',
 'variation_maxexp_oil_fuels_part',
 'maxexp_abortifacents_part_before',
 'maxexp_abortifacents_part_after',
 'maxexp_alchol_prod_before',
 'maxexp_alchol_prod_after',
 'maxexp_alcohol_part_before',
 'maxexp_alcohol_part_after',
 'maxexp_antip_landmines_part_before',
 'maxexp_antip_landmines_part_after',
 'maxexp_armament_part_before',
 'maxexp_armament_part_after',
 'maxexp_armament_prod_before',
 'maxexp_armament_prod_after',
 'maxexp_artic_oil_part_before',
 'maxexp_artic_oil_part_after',
 'maxexp_chemic_biolog_weapons_part_before',
 'maxexp_chemic_biolog_weapons_part_after',
 'maxexp_cluster_bombs_part_before',
 'maxexp_cluster_bombs_part_after',
 'maxexp_coal_mining_part_before',
 'maxexp_coal_mining_part_after',
 'maxexp_coal_mining_prod_before',
 'maxexp_coal_mining_prod_after',
 'maxexp_coal_power_gen_part_before',
 'maxexp_coal_power_gen_part_after',
 'maxexp_coal_power_gen_prod_before',
 'maxexp_coal_power_gen_prod_after',
 'maxexp_contraceptives_part_before',
 'maxexp_contraceptives_part_after',
 'maxexp_embryonic_stem_cell_research_part_before',
 'maxexp_embryonic_stem_cell_research_part_after',
 'maxexp_fossil_fuels_part_before',
 'maxexp_fossil_fuels_part_after',
 'maxexp_fossil_fuels_prod_before',
 'maxexp_fossil_fuels_prod_after',
 'maxexp_gambling_part_before',
 'maxexp_gambling_part_after',
 'maxexp_gambling_prod_before',
 'maxexp_gambling_prod_after',
 'maxexp_gmo_products_part_before',
 'maxexp_gmo_products_part_after',
 'maxexp_gmo_research_part_before',
 'maxexp_gmo_research_part_after',
 'maxexp_nuclear_weapons_part_before',
 'maxexp_nuclear_weapons_part_after',
 'maxexp_oil_sands_part_before',
 'maxexp_oil_sands_part_after',
 'maxexp_oil_sands_prod_before',
 'maxexp_oil_sands_prod_after',
 'maxexp_palm_oil_part_before',
 'maxexp_palm_oil_part_after',
 'maxexp_pornography_part_before',
 'maxexp_pornography_part_after',
 'maxexp_shale_energy_part_before',
 'maxexp_shale_energy_part_after',
 'maxexp_shale_energy_prod_before',
 'maxexp_shale_energy_prod_after',
 'maxexp_small_arms_part_before',
 'maxexp_small_arms_part_after',
 'maxexp_stem_cell_research_before',
 'maxexp_stem_cell_research_after',
 'maxexp_tobacco_part_before',
 'maxexp_tobacco_part_after',
 'maxexp_tobacco_prod_before',
 'maxexp_tobacco_prod_after',
 'maxexp_white_phosporus_part_before',
 'maxexp_white_phosporus_part_after',
 'maxexp_nuclear_weapons_prod_before',
 'maxexp_nuclear_weapons_prod_after',
 'maxexp_cluster_bombs_prod_before',
 'maxexp_cluster_bombs_prod_after',
 'maxexp_thermal_coal_prod_before',
 'maxexp_thermal_coal_prod_after',
 'maxexp_gas_fuels_prod_before',
 'maxexp_gas_fuels_prod_after',                     
 'maxexp_gas_fuels_part_before',
 'maxexp_gas_fuels_part_after',                     
 'maxexp_oil_fuels_prod_before',
 'maxexp_oil_fuels_prod_after',
 'maxexp_oil_fuels_part_before',
 'maxexp_oil_fuels_part_after']]


# %%
# e.    Select the Columns with the variation that will be add to the final Dataframe.
exposures = exposure_df[['permid','issuer_name', 'max_expo_variation']]

# f.    Merge those equities with variations with their strategies.
expostion_variation_true = exposure_df[exposure_df['max_expo_variation'] == True]

expostion_variation_true_str001 = pd.merge(expostion_variation_true, portfolio_str001, on='permid')
expostion_variation_true_str002 = pd.merge(expostion_variation_true, portfolio_str002, on='permid')
expostion_variation_true_str003 = pd.merge(expostion_variation_true, portfolio_str003, on='permid')
expostion_variation_true_str003b = pd.merge(expostion_variation_true, portfolio_str003b, on='permid')
expostion_variation_true_str004 = pd.merge(expostion_variation_true, portfolio_str004, on='permid')
expostion_variation_true_str005 = pd.merge(expostion_variation_true, portfolio_str005, on='permid')
expostion_variation_true_art8 = pd.merge(expostion_variation_true, portfolio_art8, on='permid')
expostion_variation_true_cs001 = pd.merge(expostion_variation_true, portfolio_cs001, on='permid')

# %% [markdown]
# #### 3.3. Controversies
# 
# Whenever new controversies appear, these should be add. Currently (May 2024) there are 39.

# %%
# a.    Select Relevant Datafeed’s Fields.  
controversies_column_list = ['issuer_name','permid', 'any_critical_controversy', 'controv_access_to_basic_services','controv_accounting_and_taxation','controv_animal_welfare','controv_animal_welfare_sc','controv_anticompetitive_practices',
'controv_bribery_and_corruption','controv_bribery_and_corruption_sc','controv_business_ethics','controv_business_ethics_sc','controv_carbon_impact_of_products',
'controv_community_relations','controv_community_relations_sc','controv_corporate_governance','controv_data_privacy_and_security',
'controv_emissions_effluents_and_waste','controv_emissions_effluents_and_waste_sc','controv_employees_human_rights',
'controv_employees_human_rights_sc','controv_energy_use_and_ghg_emissions','controv_energy_use_and_ghg_emissions_sc',
'controv_environmental_impact_of_products','controv_intellectual_property','controv_labour_relations','controv_labour_relations_sc',
'controv_land_use_and_biodiversity','controv_land_use_and_biodiversity_sc','controv_lobbying_and_public_policy','controv_marketing_practices',
'controv_media_ethics','controv_occupational_health_and_safety','controv_occupational_health_and_safety_sc','controv_quality_and_safety',
'controv_sanctions','controv_social_impact_of_products','controv_society_human_rights','controv_society_human_rights_sc','controv_water_use',
'controv_water_use_sc','controv_weapons']

controv_1 = df_1_filtered.loc[:, controversies_column_list]
controv_2 = df_2_filtered.loc[:, controversies_column_list]

# b.    Merge Dataframes from both Months.  
controv_df = (pd.merge(controv_1, controv_2, on='permid', suffixes=('_before', '_after'))
    .drop('issuer_name_after', axis=1) # dropping the duplicity on issuer name
    .rename(columns={'issuer_name_before': 'issuer_name'})) # rename columns

# Rellena los NA con 0, por eso las variables se homogenizan a 703 valores y muchas se mantienen en Object al no ser integer
columns_to_fill = controv_df.columns.difference(['issuer_name', 'permid'])
controv_df[columns_to_fill] = controv_df[columns_to_fill].fillna(0)

# order Columns
controv_df = sort_col_suffix(controv_df)

# Transform certain floats to integers
#controv_df.iloc[:,2:] = controv_df.iloc[:,2:].astype('float').astype('int')


# %%
# c.    Create a Variation Columnm.
controv_df = add_variation_columns(controv_df)

# Creación de las columnas de comprobación
variation_cols_contr = [col for col in controv_df.columns if col.startswith("variation_")]

# Crear columna de variación
controv_df["controv_variation"] = controv_df[variation_cols_contr].any(axis=1)

# %%
# Crear una lista con el orden deseado de las columnas
column_order = ['permid','issuer_name', 'controv_variation', 'any_critical_controversy_after'] + [col for col in controv_df.columns if col not in ['permid','issuer_name', 'controv_variation', 'any_critical_controversy_after']]

# Reordenar las columnas del dataframe según la lista creada
controv_df = controv_df.reindex(columns=column_order)

#ordenar vista 
column_order = ['permid','issuer_name', 'controv_variation', 'any_critical_controversy_after']
variation_columns = [col for col in controv_df.columns if col.startswith('variation_')]
other_columns =sorted(set(controv_df.columns)- set(column_order) - set(variation_columns))
controv_df = controv_df[column_order + variation_columns + other_columns]

controv_df = controv_df[['permid',
 'issuer_name',
 'controv_variation',
 'any_critical_controversy_after',
 'variation_any_critical_controversy',
 'variation_controv_access_to_basic_services',
 'variation_controv_accounting_and_taxation',
 'variation_controv_animal_welfare',
 'variation_controv_animal_welfare_sc',
 'variation_controv_anticompetitive_practices',
 'variation_controv_bribery_and_corruption',
 'variation_controv_bribery_and_corruption_sc',
 'variation_controv_business_ethics',
 'variation_controv_business_ethics_sc',
 'variation_controv_carbon_impact_of_products',
 'variation_controv_community_relations',
 'variation_controv_community_relations_sc',
 'variation_controv_corporate_governance',
 'variation_controv_data_privacy_and_security',
 'variation_controv_emissions_effluents_and_waste',
 'variation_controv_emissions_effluents_and_waste_sc',
 'variation_controv_employees_human_rights',
 'variation_controv_employees_human_rights_sc',
 'variation_controv_energy_use_and_ghg_emissions',
 'variation_controv_energy_use_and_ghg_emissions_sc',
 'variation_controv_environmental_impact_of_products',
 'variation_controv_intellectual_property',
 'variation_controv_labour_relations',
 'variation_controv_labour_relations_sc',
 'variation_controv_land_use_and_biodiversity',
 'variation_controv_land_use_and_biodiversity_sc',
 'variation_controv_lobbying_and_public_policy',
 'variation_controv_marketing_practices',
 'variation_controv_media_ethics',
 'variation_controv_occupational_health_and_safety',
 'variation_controv_occupational_health_and_safety_sc',
 'variation_controv_quality_and_safety',
 'variation_controv_sanctions',
 'variation_controv_social_impact_of_products',
 'variation_controv_society_human_rights',
 'variation_controv_society_human_rights_sc',
 'variation_controv_water_use',
 'variation_controv_water_use_sc',
 'variation_controv_weapons',
 'controv_access_to_basic_services_before',                                                     
 'controv_access_to_basic_services_after',
 'controv_accounting_and_taxation_before',
 'controv_accounting_and_taxation_after',
 'controv_animal_welfare_before',
 'controv_animal_welfare_after',
 'controv_animal_welfare_sc_before',
 'controv_animal_welfare_sc_after',
 'controv_anticompetitive_practices_before',
 'controv_anticompetitive_practices_after',
 'controv_bribery_and_corruption_before',
 'controv_bribery_and_corruption_after',
 'controv_bribery_and_corruption_sc_before',
 'controv_bribery_and_corruption_sc_after',
 'controv_business_ethics_before',
 'controv_business_ethics_after',
 'controv_business_ethics_sc_before',
 'controv_business_ethics_sc_after',
 'controv_carbon_impact_of_products_before',
 'controv_carbon_impact_of_products_after',
 'controv_community_relations_before',
 'controv_community_relations_after',
 'controv_community_relations_sc_before',
 'controv_community_relations_sc_after',
 'controv_corporate_governance_before',
 'controv_corporate_governance_after',
 'controv_data_privacy_and_security_before',
 'controv_data_privacy_and_security_after',
 'controv_emissions_effluents_and_waste_before',
 'controv_emissions_effluents_and_waste_after',
 'controv_emissions_effluents_and_waste_sc_before',
 'controv_emissions_effluents_and_waste_sc_after',
 'controv_employees_human_rights_before',
 'controv_employees_human_rights_after',
 'controv_employees_human_rights_sc_before',
 'controv_employees_human_rights_sc_after',
 'controv_energy_use_and_ghg_emissions_before',
 'controv_energy_use_and_ghg_emissions_after',
 'controv_energy_use_and_ghg_emissions_sc_before',
 'controv_energy_use_and_ghg_emissions_sc_after',
 'controv_environmental_impact_of_products_before',
 'controv_environmental_impact_of_products_after',
 'controv_intellectual_property_before',
 'controv_intellectual_property_after',
 'controv_labour_relations_before',
 'controv_labour_relations_after',
 'controv_labour_relations_sc_before',
 'controv_labour_relations_sc_after',
 'controv_land_use_and_biodiversity_before',
 'controv_land_use_and_biodiversity_after',
 'controv_land_use_and_biodiversity_sc_before',
 'controv_land_use_and_biodiversity_sc_after',
 'controv_lobbying_and_public_policy_before',
 'controv_lobbying_and_public_policy_after',
 'controv_marketing_practices_before',
 'controv_marketing_practices_after',
 'controv_media_ethics_before',
 'controv_media_ethics_after',
 'controv_occupational_health_and_safety_before',
 'controv_occupational_health_and_safety_after',
 'controv_occupational_health_and_safety_sc_before',
 'controv_occupational_health_and_safety_sc_after',
 'controv_quality_and_safety_before',
 'controv_quality_and_safety_after',
 'controv_sanctions_before',
 'controv_sanctions_after',
 'controv_social_impact_of_products_before',
 'controv_social_impact_of_products_after',
 'controv_society_human_rights_before',
 'controv_society_human_rights_after',
 'controv_society_human_rights_sc_before',
 'controv_society_human_rights_sc_after',
 'controv_water_use_before',
 'controv_water_use_after',
 'controv_water_use_sc_before',
 'controv_water_use_sc_after',
 'controv_weapons_before',
 'controv_weapons_after']]


#controversies_df.to_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/Análisis_Cambios_OVR/2024_04/n_df_Cont.xlsx', sheet_name='Resumen', index=False)


# %%
# e.	Select the Columns with the variation that will be add to the final Dataframe.  
controversies_variation_true = controv_df[controv_df['controv_variation']==True]

controversias = controversies_variation_true[[ 'permid','issuer_name','controv_variation']]

# f.	Merge those equities with variations with their strategies. 
controversies_variation_str001 = pd.merge(controversies_variation_true, portfolio_str001, on='permid')
controversies_variation_str002 = pd.merge(controversies_variation_true, portfolio_str002, on='permid')
controversies_variation_str003 = pd.merge(controversies_variation_true, portfolio_str003, on='permid')
controversies_variation_str003b = pd.merge(controversies_variation_true, portfolio_str003b, on='permid')
controversies_variation_str004 = pd.merge(controversies_variation_true, portfolio_str004, on='permid')
controversies_variation_str005 = pd.merge(controversies_variation_true, portfolio_str005, on='permid')
controversies_variation_art8 = pd.merge(controversies_variation_true, portfolio_art8, on='permid')
controversies_variation_cs001 = pd.merge(controversies_variation_true, portfolio_cs001, on='permid')

# %% [markdown]
# #### 3.4. Strategies

# %%
# a.	Select Relevant Datafeed’s Fields.  
strategies_1 = df_1_filtered.loc[:, ['issuer_name','permid','str_001_s','str_002_ec','str_003_ec','str_004_asec','str_005_ec','cs_001_sec','cs_003_sec','cs_002_ec','str_006_sec','str_007_sect','art_8_basicos','str_003b_ec','gp_esccp_22']]
strategies_2 = df_2_filtered.loc[:, ['issuer_name','permid','str_001_s','str_002_ec','str_003_ec','str_004_asec','str_005_ec','cs_001_sec','cs_003_sec','cs_002_ec','str_006_sec','str_007_sect','art_8_basicos','str_003b_ec','gp_esccp_22']]

# b.	Merge Dataframes from both Months.  
strategies_df = pd.merge(strategies_1, strategies_2, on='permid', suffixes=('_before', '_after'))
strategies_df.drop(['issuer_name_after'], axis = 'columns', inplace=True)
strategies_df.rename(columns = {'issuer_name_before':'issuer_name'},inplace =True)

columns_to_fill = strategies_df.columns.difference(['permid','issuer_name'])
strategies_df[columns_to_fill] = strategies_df[columns_to_fill].fillna(0)

# c.    order Columns
strategies_df = sort_col_suffix(strategies_df)

# %%
# d.    Create a Variation Columnm.
strategies_df = add_variation_columns(strategies_df)

variation_cols_estrat = [col for col in strategies_df.columns if col.startswith("variation_")]

# Crear columna de variación
strategies_df["variation_estrategias"] = strategies_df[variation_cols_estrat].any(axis=1)

# %%
# Crear una lista con el orden deseado de las columnas
column_order = ['permid','issuer_name', 'variation_estrategias'] + [col for col in strategies_df.columns if col not in ['permid','issuer_name', 'variation_estrategias']]

# Reordenar las columnas del dataframe según la lista creada
strategies_df = strategies_df.reindex(columns=column_order)

column_order_es = ['permid','issuer_name']
variation_columns_es = [col for col in strategies_df.columns if col.startswith('variation_')]
other_columns_es     = sorted(set(strategies_df.columns)- set(column_order_es) - set(variation_columns_es))
strategies_df  = strategies_df[column_order_es + variation_columns_es + other_columns_es]

strategies_df = strategies_df [['permid',
 'issuer_name',
 'variation_estrategias',
 'variation_art_8_basicos',
 'variation_cs_001_sec',
 'variation_cs_002_ec',
 'variation_cs_003_sec',
 'variation_gp_esccp_22',                   
 'variation_str_001_s',
 'variation_str_002_ec',
 'variation_str_003_ec',
 'variation_str_003b_ec',
 'variation_str_004_asec',
 'variation_str_005_ec',
 'variation_str_006_sec',
 'variation_str_007_sect',
 'art_8_basicos_before',                  
 'art_8_basicos_after',
 'cs_001_sec_before',                   
 'cs_001_sec_after',
 'cs_002_ec_before',
 'cs_002_ec_after',
 'cs_003_sec_before',
 'cs_003_sec_after',
 'str_001_s_before',
 'str_001_s_after',
 'str_002_ec_before',
 'str_002_ec_after',
 'str_003_ec_before',
 'str_003_ec_after',
 'str_004_asec_before',
 'str_004_asec_after',
 'str_005_ec_before',
 'str_005_ec_after',
 'str_006_sec_before',
 'str_006_sec_after',
 'str_007_sect_before',
 'str_007_sect_after',
 'str_003b_ec_before',
 'str_003b_ec_after',
 'gp_esccp_22_before',
 'gp_esccp_22_after']]

# %%
# e.	Select the Columns with the variation that will be add to the final Dataframe.  
estrategias = strategies_df[['permid','issuer_name','variation_estrategias']]

# f.	Merge those equities with variations with their strategies.  
estrategias_true = strategies_df[strategies_df['variation_estrategias'] == True]

estrategias_true_str001 = pd.merge(estrategias_true, portfolio_str001, on='permid')
estrategias_true_str002 = pd.merge(estrategias_true, portfolio_str002, on='permid')
estrategias_true_str003 = pd.merge(estrategias_true, portfolio_str003, on='permid')
estrategias_true_str003b = pd.merge(estrategias_true, portfolio_str003b, on='permid')
estrategias_true_str004 = pd.merge(estrategias_true, portfolio_str004, on='permid')
estrategias_true_str005 = pd.merge(estrategias_true, portfolio_str005, on='permid')
estrategias_true_art8 = pd.merge(estrategias_true, portfolio_art8, on='permid')
estrategias_true_cs001 = pd.merge(estrategias_true, portfolio_cs001, on='permid')


# %%
#estrategias_true_str004.to_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/Análisis_Cambios_OVR/2024_04/estrategias_true_str004.xlsx', sheet_name='Resumen', index=False)
#Estrategias.to_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/Análisis_Cambios_OVR/2024_04/Estrategias.xlsx', sheet_name='Resumen', index=False)

# %% [markdown]
# ### 4. Gather and Export Variations.

# %%
# Define the DataFrames to be merged along with their keys
dataframes = [
    (sustainability_rating[['permid', 'sust_rating_variation']], 'permid'),
    (controversias[['permid', 'controv_variation']], 'permid'),
    (estrategias[['permid', 'variation_estrategias']], 'permid')
]
 
# Start with the exposures DataFrame
data = exposures
 
# Merge each DataFrame in the list
for df, key in dataframes:
    data = pd.merge(data, df, on=key, how='outer')

# %%
data.rename(columns = {'max_expo_variation':'variation_max_expo', 'max_expo_variation':'variation_max_expo', 'controv_variation':'variation_controv'},inplace =True)

# %%
variation_df_cols = [col for col in data.columns if col.startswith("variation_")]

# Crear columna de variación
data["variation_df"] = data[variation_df_cols].any(axis=1)

# %%
data_str001 = pd.merge(data, portfolio_str001, on='permid')
data_str002 = pd.merge(data, portfolio_str002, on='permid')
data_str003 = pd.merge(data, portfolio_str003, on='permid')
data_str003b = pd.merge(data, portfolio_str003b, on='permid')
data_str004 = pd.merge(data, portfolio_str004, on='permid')
data_str005 = pd.merge(data, portfolio_str005, on='permid')
data_art8 = pd.merge(data, portfolio_art8, on='permid')
data_cs001 = pd.merge(data, portfolio_cs001, on='permid')

# %% [markdown]
# ## FICHERO FINAL
# 
# ## Cambiar antes de ejecutar, path y nombre: por lo general ya se ha cambiado nombre 
#    - data = Fichero final resumen 
#    
#    - `data`.to_excel(r'X:/INVDESPRO/INVESTIGACION/Fondos éticos/Misbel/Pruebas/**`Resumen_Variaciones_'`**,index=False)

# %%
hoja_str001_1 = data_str001
hoja_str001_2 = sust_rat_variation_true_str001
hoja_str001_3 = expostion_variation_true_str001
hoja_str001_4 = controversies_variation_str001
hoja_str001_5 = estrategias_true_str001

hoja_str002_1 = data_str002
hoja_str002_2 = sust_rat_variation_true_str002
hoja_str002_3 = expostion_variation_true_str002
hoja_str002_4 = controversies_variation_str002
hoja_str002_5 = estrategias_true_str002
 
hoja_str003_1 = data_str003
hoja_str003_2 = sust_rat_variation_true_str003
hoja_str003_3 = expostion_variation_true_str003
hoja_str003_4 = controversies_variation_str003
hoja_str003_5 = estrategias_true_str003

hoja_str003b_1 = data_str003b
hoja_str003b_2 = sust_rat_variation_true_str003b
hoja_str003b_3 = expostion_variation_true_str003b
hoja_str003b_4 = controversies_variation_str003b
hoja_str003b_5 = estrategias_true_str003b

hoja_str004_1 = data_str004
hoja_str004_2 = sust_rat_variation_true_str004
hoja_str004_3 = expostion_variation_true_str004
hoja_str004_4 = controversies_variation_str004
hoja_str004_5 = estrategias_true_str004
 
hoja_str005_1 = data_str005
hoja_str005_2 = sust_rat_variation_true_str005
hoja_str005_3 = expostion_variation_true_str005
hoja_str005_4 = controversies_variation_str005
hoja_str005_5 = estrategias_true_str005

hoja_art8_1 = data_art8
hoja_art8_2 = sust_rat_variation_true_art8
hoja_art8_3 = expostion_variation_true_art8
hoja_art8_4 = controversies_variation_art8
hoja_art8_5 = estrategias_true_art8

hoja_cs001_1 = data_cs001
hoja_cs001_2 = sust_rat_variation_true_cs001
hoja_cs001_3 = expostion_variation_true_cs001
hoja_cs001_4 = controversies_variation_cs001
hoja_cs001_5 = estrategias_true_cs001

# %% [markdown]
# # CAMBIAR RUTA Y NOMBRE (antes de ejecutar)
# 

# %%
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter, range_boundaries

# Define the base directory and common path structure
base_dir = rf"C:\Users\n740789\Documents\Projects_local\DataSets\overwrites\analisis_cambios_ovr\{DATE}\\"

paths = {
    'STR001.xlsx': [hoja_str001_1, hoja_str001_2, hoja_str001_3, hoja_str001_4, hoja_str001_5],
    'STR002.xlsx': [hoja_str002_1, hoja_str002_2, hoja_str002_3, hoja_str002_4, hoja_str002_5],
    'STR003.xlsx': [hoja_str003_1, hoja_str003_2, hoja_str003_3, hoja_str003_4, hoja_str003_5],
    'STR003B.xlsx': [hoja_str003b_1, hoja_str003b_2, hoja_str003b_3, hoja_str003b_4, hoja_str003b_5],
    'STR004.xlsx': [hoja_str004_1, hoja_str004_2, hoja_str004_3, hoja_str004_4, hoja_str004_5],
    'STR005.xlsx': [hoja_str005_1, hoja_str005_2, hoja_str005_3, hoja_str005_4, hoja_str005_5],
    'ART8.xlsx': [hoja_art8_1, hoja_art8_2, hoja_art8_3, hoja_art8_4, hoja_art8_5],
    'CS001.xlsx': [hoja_cs001_1, hoja_cs001_2, hoja_cs001_3, hoja_cs001_4, hoja_cs001_5]
}
 
sheet_names = ['Resumen', 'Sust_R', 'Max_Exp', 'Controversias', 'Estrategias']
 
# Process each file with a loop
for file_name, sheets in paths.items():
    full_path = base_dir + file_name
    with pd.ExcelWriter(full_path) as writer:
        for sheet, data in zip(sheet_names, sheets):
            data.to_excel(writer, sheet_name=sheet, index=False)

# %%
# add variabel $date or ${date[:-2]}
# Whatcout, the script read the Data Base from Overwrites May 2024. If that that base was updated the script should also be updated. 
#!python "C:/Users/n740789/Documents/Projects_local/python_scripts/overrides_v3.py" {DATE}

# %%
end = time.time()
print(f"{end - start:.2f} seconds")


