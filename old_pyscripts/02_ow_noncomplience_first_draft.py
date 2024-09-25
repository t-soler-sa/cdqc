import numpy as np
import pandas as pd

# Read Datafeed, Crossrefenrence, Portfolio/Cartera, and Overwrites

#DATA FEED CLARITY,
path_df = r"C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\DATAFEED\ficheros_tratados\t_20240601_Equities_feed_IssuerLevel_sinOVR_v7.csv"
datafeed = pd.read_csv(path_df, sep=',', dtype='unicode')

## Crossrefenrene
xref =pd.read_csv(r'X:\INVDESPRO\INVESTIGACION\Fondos éticos\3. DATASETS\03_Cross Reference\2024\Aladdin_Clarity_Issuers_20240601.csv', dtype={'CLARITY_AI': str})

## portfolios/ Carteras
car = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/10. PROVEEDORES/01_Aladdin/Descargas_SNT-WORLD/c_202406.xlsx', sheet_name =None, skiprows=6)
car = pd.concat(car.values(), ignore_index=True)

## Overwrites

#Read 1 OW
CS_001_OW   = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/CS001_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 2 OW
CS_002_OW   = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/CS002_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 3 OW
CS_003_OW   = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/CS003_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 4 OW
STR_001_OW  = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR001_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 5 OW
STR_002_OW  = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR002_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 6 OW
STR_003_OW  = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR003_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 7 OW
STR_004_OW  = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR004_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 8 OW
STR_005_OW  = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR005_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 9 OW
STR_006_OW  = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR006_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 10 OW
STR_007_OW   = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR007_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 11 OW
STR_SFDR8_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/ARTICULO_8_SEC_202406.xlsx', dtype={'ClarityID': str})

#Read 12 OW
STR_003B_OW  = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202406_OVR_June/STR003B_SEC_202406.xlsx', dtype={'ClarityID': str})

# Edit column names
for dfs in [datafeed, xref, car, CS_001_OW,  CS_002_OW, CS_003_OW, STR_001_OW, STR_002_OW, STR_003_OW, STR_004_OW, STR_005_OW, STR_006_OW, STR_007_OW, STR_SFDR8_OW, STR_003B_OW]:
    dfs.columns = dfs.columns.str.strip().str.lower()

# 4. Al final del archivo cambiar ruta o nombre 
# 5. Incumplimientos
# 6. Run all o paso a paso, luego de los 4 pasos anteriores 

# Verificación de carga DF, datos 

#DF.columns Ubicacación de cada columna a verificar, que se van a cambiar por la validación
##array([242, 245, 244, 236, 237, 238, 239, 241, 246, 247, 251, 254], dtype=int64) este es el resultado esperado 

datafeed.columns.get_indexer(['cs_001_sec','cs_002_ec','cs_003_sec','str_001_s','str_002_ec','str_003_ec','str_004_asec','str_005_ec','str_006_sec','str_007_sect','art_8_basicos','str_003b_ec'])

def process_ow(df, clarity_col, ow_df, ow_col, new_col_name, insert_loc):
    # Prepare the dataframe for merging
    clarity_df = df[['clarityid', clarity_col]]
    
    # Merge with OW dataframe
    ow_merged = pd.merge(clarity_df, ow_df, how='left', on='clarityid')
    
    # Create the new verification column
    ow_merged[new_col_name] = np.where(ow_merged[ow_col] != ow_merged[clarity_col], ow_merged[ow_col], ow_merged[clarity_col])
    
    # Fill NaN values
    ow_merged[new_col_name] = ow_merged[new_col_name].fillna(ow_merged[clarity_col])
    
    # Drop unnecessary columns
    ow_merged.drop(['clarityid', clarity_col, ow_col], axis='columns', inplace=True)
    
    # Insert the new column into the original dataframe
    df.insert(insert_loc, new_col_name, ow_merged[new_col_name])
    
    # Drop the original column from the original dataframe
    df.drop([clarity_col], axis='columns', inplace=True)

# Process each OW case
ow_cases = [
    ('cs_001_sec', CS_001_OW, 'cs001', 'cs_001_sec_new', 240),
    ('cs_002_ec', CS_002_OW, 'cs002', 'cs_002_ec_new', 243),
    ('cs_003_sec', CS_003_OW, 'cs003', 'cs_003_sec_new', 242),
    ('str_001_s', STR_001_OW, 'str001', 'str_001_s_new', 234),
    ('str_002_ec', STR_002_OW, 'str002', 'str_002_ec_new', 235),
    ('str_003_ec', STR_003_OW, 'str003', 'str_003_ec_new', 236),
    ('str_004_asec', STR_004_OW, 'str004', 'str_004_asec_new', 237),
    ('str_005_ec', STR_005_OW, 'str005', 'str_005_ec_new', 239),
    ('str_006_sec', STR_006_OW, 'str006', 'str_006_sec_new', 244),
    ('str_007_sect', STR_007_OW, 'str007', 'str_007_sect_new', 245),
    ('art_8_basicos', STR_SFDR8_OW, 'art_8_basicos', 'art_8_basicos_new', 249),
    ('str_003b_ec', STR_003B_OW, 'str003b', 'str_003b_ec_new', 252),
]

for clarity_col, ow_df, ow_col, new_col_name, insert_loc in ow_cases:
    process_ow(datafeed, clarity_col, ow_df, ow_col, new_col_name, insert_loc)

#NUEVO CSV, indicar nuevo nombre y que deberia ser siempre, sobre escribir , agregarle,  mode='w',sino se va a cambiar el nombre 

# 5 Incumplimientos

## Filtro para incumplimientos
datafeed.columns = datafeed.columns.str.replace(r'_new$', '', regex=True)
#Filtrar el DF solo la col que necesito
df_filtered = datafeed[['permid','clarityid','issuer_name','str_001_s',
                  'str_002_ec','str_003_ec','str_004_asec','str_005_ec','str_006_sec',
                  'str_007_sect','cs_001_sec','cs_002_ec','cs_003_sec','art_8_basicos','gp_esccp_22','str_003b_ec']].copy()

carteras_filtered = car.iloc[:,[0,3,4]]
carteras_filtered.rename(columns={"security description":"issuername", "issuer id":"issuerid", "portfolio name":"portfolio"}, inplace=True)

#Cambio de nombre, orden y tipo en el cross reference
xref.rename(columns={"clarity_ai":"permid", "issuer_name":"issuername", "aladdin_issuer":"issuerid"}, inplace=True)
xref = xref[['permid','issuerid', 'issuername']]

def process_portfolio(carteras_filtered, df_filtered, xref, portfolio_list, str_column, str_name):
    xref_b = xref.copy()
    df_filtered_b = df_filtered.copy()
    carteras_filtered_b = carteras_filtered.copy()

    result = carteras_filtered_b[carteras_filtered_b['portfolio'].isin(portfolio_list)].dropna().copy()
    result.columns = ['issuername', 'issuerid', 'portfolio']
    
    df = df_filtered_b[['permid', str_column]].copy()
    df.columns = ['permid', str_name]
    
    df = pd.merge(df, xref_b, on='permid', how='left')
    result = pd.merge(result, df, on='issuerid', how='left')
    
    result = result.drop(columns=['permid', 'issuername_y']).drop_duplicates().dropna(subset=[str_name])
    result = result.rename(columns={'issuername_x': 'issuername', str_name: str_name})
    result = result[result[str_name] == 'EXCLUDED']
    return result[['issuerid', 'issuername', 'portfolio', str_name]]

STR001 = process_portfolio(carteras_filtered, df_filtered, xref, 
                           ["FPG00002","FPG01072","EPV00005","EPV14005", "FPG01079","FPG01697","FPG00112","EPV00043","EPV14004","LXMS0720",
                            "FPG00028", "FPG01214", "FPG00027", "FPG00603", "FPG01213", "EPV00060", "EPV00061", "EPV00062", "EPV00063", "EPV00064"], 
                           'str_001_s', 'STR001')

STR002 = process_portfolio(carteras_filtered, df_filtered, xref, 
                           ["FIG02787", "CPE03861"], 
                           'str_002_ec', 'STR002')

STR003 = process_portfolio(carteras_filtered, df_filtered, xref, 
                           ["FPG01278"], 
                           'str_003_ec', 'STR003')

STR004 = process_portfolio(carteras_filtered, df_filtered, xref, 
                           ["FPH00457", "EPH00107", "FIG05273", "FPB01158", "FIG05240", "FIG05241", "FIG05402", "FIG00677", "PFI01541"], 
                           'str_004_asec', 'STR004')

STR004_SB = process_portfolio(carteras_filtered, df_filtered, xref, 
                              ["FPB01158", "FIG05240", "FIG05241", "FIG05402", "FIG00677", "PFI01541"], 
                              'str_004_asec', 'STR004_SB')

STR005 = process_portfolio(carteras_filtered, df_filtered, xref, 
                           ["FIH00529", "CPE04024"], 
                           'str_005_ec', 'STR005')

STR006 = process_portfolio(carteras_filtered, df_filtered, xref, 
                           ["LXMS0660", "CL19778"], 
                           'str_006_sec', 'STR006')

STR007 = process_portfolio(carteras_filtered, df_filtered, xref, 
                           ["FIG05301"], 
                           'str_007_sect', 'STR007')

CS001 = process_portfolio(carteras_filtered, df_filtered, xref, 
                          ["CPE03744"], 
                          'cs_001_sec', 'CS001')

ART8 = process_portfolio(carteras_filtered, df_filtered, xref, 
                         ["FPG00015", "FPG01421", "FPH00254", "EPV00052", "EPV00051", "EPV00050", "FPG00026", "FPG00555", "EPV14006",
                           "FIF00154", "FIF04284", "FIF04285", "FIG00026", "FIG00689", "FIG01973", "FIG01988", "FIG01998", "FIG02164",
                            "FIG02410", "FIG04251", "FIG04253", "FIG04868", "FIG05178", "FIG05198", "FIG05415", "FIG05428", "FIG05698",
                            "FIH00058", "FIH00208", "FIH01197", "FIH01920", "FIN00315", "FIN00441", "FIN00637", "CMD07211", "CMD07212", 
                            "CMD07213", "CMD07214", "CMD07215", "CMD08211", "CMD08212", "CMD08213", "CMD08214", "CMD08215", "PFIT0042", 
                            "PFIT0516", "PFIT0605", "PFIT0647", "PFIT1421", "PFIT1422", "PFIT1423", "PFIT1424", "PFIT1425", "PFIT1426", 
                            "PFIT1830", "PFS00001", "PFS00002", "PFS00058", "PFS00059", "PFS00358", "PFS00359", "PFS00360", "PFS00365", 
                            "PFS00366", "PFS00371", "PFSULM01", "PFSULM02", "PFSULM03", "PFSULM04", "PFSULM05", "LXMS0020", "LXMS0040", 
                            "LXMS0240", "LXMS0360", "LXMS0520", "LXMS0530", "LXMS0540", "LXMS0340", "LXMS0350", "LXMS0650", "LXMS0630", 
                            "LXMS0550", "LXMS0560", "LXMS0570", "PFI01549", "PFIT0011", "FPG00687", "FPG01066", "LXMS0580", "FIG05774", 
                            "LXMS0730"], 
                         'art_8_basicos', 'ART8')

STR003B = process_portfolio(carteras_filtered, df_filtered, xref, 
                            ["CPE00035", "CPE00169", "CPE00264", "CPE00277", "CPE00363", "CPE00375", "CPE00448", "CPE00456", "CPE00464", 
                             "CPE03002", "CPE03290", "CPE03519", "CPE03570", "CPE03853", "CPE03867", "CPE03912", "CPE03913", "CPE03928", 
                             "CPE03986", "CPE04036", "CPE04037", "CPE04039", "CPE04041", "CPE04044", "CPE04053", "CPE04059", "CPE04063", 
                             "CPE04070", "CPE04071", "CPE04075", "CPE04076", "CPE04077", "CPE04080", "CPE04081", "CPE04082", "CPE04084", 
                             "CPE04087", "CPE05134", "CPE05137", "CPE05148", "CPE05150", "CPE05151", "CPE05152", "CPE05162", "CPE05174", 
                             "CPE05177", "CPE05180", "CPE05189", "CPE05190", "CPE05191", "CPE05192", "CPE05207", "CPE05208", "CPE05211", 
                             "CPE05212", "CPE05218", "CPE05219", "CPE05226", "CPE05234", "CPE05239", "CPE05242", "CPE05244", "CPE05245", 
                             "CPE05248", "CPE05252", "CPE05258", "CPE05261", "CPE05314", "CPE05315", "CPE05317", "CPE05318", "CPE05337", 
                             "CPE05340", "CPE05341", "CPE05344", "CPE05351", "CPE05355", "CPE05361", "CPE05362", "CPE05363", "CPE05366"], 
                            'str_003b_ec', 'STR003B')



dfs_list = [STR001, STR002, STR003, STR004, STR004_SB, STR005, STR006, STR007, CS001, ART8, STR003B]

# Fichero Incumplimientos

#Edit month accordingly
month_date = "202406"

extension = ".csv"
path = "X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_" + month_date + extension

dfs_list = [STR001, STR002, STR003, STR004, STR004_SB, STR005, STR006, STR007, CS001, ART8, STR003B]

# Edit column names for the export format
for datafeed in dfs_list:
    datafeed["strategy_name"] = datafeed.columns[-1]
    datafeed["date"] = month_date
    datafeed.rename(columns={datafeed.columns[-3]: "resultado"}, inplace=True)

df_export = pd.concat(dfs_list, ignore_index=True)
grouped_df_export = df_export.groupby(["strategy_name","issuername", "issuerid", "date"])["portfolio"].apply(list).reset_index()

# export results to a csv file
STR001.to_csv(path, index=False, header=True)
for datafeed in [STR002, STR003, STR004, STR004_SB, STR005, STR006, STR007, CS001, ART8, STR003B]:    
    datafeed.to_csv(path, index=False, mode='a', header=False)

# export results to a csv file with alternative format
path_2 = "X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_v2_" + month_date + extension
grouped_df_export.to_csv(path_2, index=False, header=True)