import numpy as np
import pandas as pd

#DATA FEED CLARITY, actual,copiar ruta del mes en curso 

#TODO DEBE ESTAR EN '/' 

#crossreference del mes 

crossreference=pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/03_Cross Reference/20240201_Aladdin_Clarity_Issuers.xlsx')

#Carteras, sustituir por cartera a utilizar --> `C:/Users/nXXXXXX/Downloads/xxxxxx.xlsx`. Descargar de Aladdin (Workspace: Descarga_Incumplimientos)
file_path= 'C:/Users/n629352/Downloads/Risk and exposure - 2024-02-13T101801.182.xlsx'
Carteras =pd.read_excel(file_path, sheet_name =None, skiprows=4)

Carteras= pd.concat(Carteras.values(), ignore_index=True)

# ## 5

# # Incumplimientos

#DATA FEED LUEGO DE APLICAR OW
DF = pd.read_csv('C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ow\202407_DF_July.csv')

## Filtro para incumplimientos

#Filtrar el DF solo la col que necesito
DF_filtered = DF[['permId','ClarityID','issuer_name','str_001_s','str_002_ec','str_003_ec','str_004_asec','str_005_ec','str_006_sec','str_007_sect','cs_001_sec','cs_002_ec','cs_003_sec','art_8_basicos','gp_esccp_22','str_003b_ec']]

carteras_filtered=Carteras.iloc[:,[0,3,4]]
carteras_filtered.columns = ['IssuerID', 'IssuerName','Portfolio']

#Cambio de nombre, orden y tipo
crossreference.columns= ['IssuerID', 'IssuerName', 'permId']
crossreference= crossreference[['permId','IssuerID', 'IssuerName']]
crossreference['permId']= crossreference['permId'].astype('int64')

#STR001
carteras_filtered_1= carteras_filtered.copy()
conditions= carteras_filtered_1['Portfolio'].isin(["FPG00002","FPG01072","EPV00005","EPV14005","FPG01079","FPG01697","FPG00112","EPV00043","EPV14004","LXMS0720"])

STR001= carteras_filtered_1[conditions].dropna().copy()#elimino datos vacios null
STR001.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF001 = DF_filtered[['permId','str_001_s']].copy()
DF001.columns =['permId','STR_001']
DF001['permId']=DF001['permId'].astype('int64')

DF001 =pd.merge(DF001,crossreference, on='permId', how='left')

STR001 =pd.merge(STR001,DF001, on='IssuerID', how='left')
STR001= STR001.drop(columns=['permId', 'IssuerName_y']).drop_duplicates().dropna(subset=['STR_001'])

STR001= STR001.rename(columns={'IssuerName_x':'IssuerName', 'STR_001':'STR001'})
STR001= STR001[STR001['STR001']== 'EXCLUDED']
STR001= STR001[['IssuerID', 'IssuerName','Portfolio','STR001']]
# ## STR002


#STR002
conditions_2= carteras_filtered['Portfolio'].isin(["FIG02787","CPE03861"])
STR002= carteras_filtered[conditions_2].dropna().copy()
STR002.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF002 = DF_filtered[['permId','str_002_ec']].copy()
DF002.columns =['permId','STR_002']
DF002['permId']=DF002['permId'].astype('int64')


DF002 =pd.merge(DF002,crossreference, on='permId', how='left')

STR002 =pd.merge(STR002,DF002, on='IssuerID', how='left')
STR002= STR002.drop(columns=['permId', 'IssuerName_y']).drop_duplicates().dropna(subset=['STR_002'])

STR002= STR002.rename(columns={'IssuerName_x':'IssuerName', 'STR_002':'STR002'})
STR002= STR002[STR002['STR002']== 'EXCLUDED']
STR002= STR002[['IssuerID', 'IssuerName','Portfolio','STR002']]

# ## STR003

#STR003
conditions_3= carteras_filtered['Portfolio'].isin(["FPG01278"])


STR003= carteras_filtered[conditions_3].dropna().copy()
STR003.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF003 = DF_filtered[['permId','str_003_ec']].copy()
DF003.columns =['permId','STR_003']
DF003['permId']=DF003['permId'].astype('int64')

DF003 =pd.merge(DF003,crossreference, on='permId', how='left')
DF003.info()
DF003.head()


STR003 =pd.merge(STR003,DF003, on='IssuerID', how='left')
STR003= STR003.drop(columns=['permId', 'IssuerName_y']).drop_duplicates().dropna(subset=['STR_003'])

STR003= STR003.rename(columns={'IssuerName_x':'IssuerName', 'STR_003':'STR003'})
STR003= STR003[STR003['STR003']== 'EXCLUDED']
STR003= STR003[['IssuerID', 'IssuerName','Portfolio','STR003']]

# ## STR004

#STR004
carteras_filtered
conditions_4= carteras_filtered['Portfolio'].isin(["FPH00457","EPH00107","FPG00028","FPG01214","FPG00027","FPG00603","FPG01213","EPV00060","EPV00064","EPV00063","EPV00062","EPV00061","FIG05273"])


STR004= carteras_filtered[conditions_4].dropna().copy()
STR004.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF004 = DF_filtered[['permId','str_004_asec']].copy()
DF004.columns =['permId','STR004']
DF004['permId']=DF004['permId'].astype('int64')


DF004 =pd.merge(DF004,crossreference, on='permId', how='left')

STR004 =pd.merge(STR004,DF004, on='IssuerID', how='left')
STR004= STR004.drop(columns=['permId', 'IssuerName_y']).drop_duplicates().dropna(subset=['STR004'])

STR004= STR004.rename(columns={'IssuerName_x':'IssuerName'})
STR004= STR004[STR004['STR004']== 'EXCLUDED']
STR004= STR004[['IssuerID', 'IssuerName','Portfolio','STR004']]

# ## STR004_SB

#STR004_SB
carteras_filtered_4sb=carteras_filtered.copy()
conditions_4sb= carteras_filtered_4sb['Portfolio'].isin(["FPB01158","FIG05240","FIG05241","FIG05402","FIG00677","PFI01541"])


STR004_SB= carteras_filtered_4sb[conditions_4sb].dropna().copy()
STR004_SB.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF004_SB = DF_filtered[['permId','str_004_asec']].copy()
DF004_SB.columns =['permId','STR_004_SB']
DF004_SB['permId']=DF004_SB['permId'].astype('int64')

DF004_SB =pd.merge(DF004_SB,crossreference, on='permId', how='left')
DF004_SB =DF004_SB [['IssuerID','permId', 'IssuerName', 'STR_004_SB']]

STR004_SB =pd.merge(STR004_SB,DF004_SB, on='IssuerID', how='left')
STR004_SB= STR004_SB.drop(columns=['permId', 'IssuerName_y']).drop_duplicates().dropna(subset=['STR_004_SB'])

STR004_SB= STR004_SB.rename(columns={'IssuerName_x':'IssuerName', 'STR_004_SB':'STR004_SB'})
STR004_SB= STR004_SB[STR004_SB['STR004_SB']== 'EXCLUDED']
STR004_SB= STR004_SB[['IssuerID','IssuerName','Portfolio','STR004_SB']]

# ## STR005

#STR005
carteras_filtered_5=carteras_filtered.copy()
conditions_5= carteras_filtered_5['Portfolio'].isin(["FIH00529","CPE04024"])


STR005= carteras_filtered_5[conditions_5].dropna().copy()
STR005.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF005 = DF_filtered[['permId','str_005_ec']].copy()
DF005.columns =['permId','STR_005']
DF005['permId']=DF005['permId'].astype('int64')

DF005 =pd.merge(DF005,crossreference, on='permId', how='left')
DF005 =DF005 [['IssuerID','permId', 'IssuerName', 'STR_005']]

STR005 =pd.merge(STR005,DF005, on='IssuerID', how='left')
STR005= STR005.drop(columns=['permId','IssuerName_y']).drop_duplicates().dropna(subset=['STR_005'])

STR005= STR005.rename(columns={'IssuerName_x':'IssuerName','STR_005':'STR005'})
STR005= STR005[STR005['STR005']== 'EXCLUDED']
STR005= STR005[['IssuerID','IssuerName','Portfolio','STR005']]

# ## STR006

#STR006
carteras_filtered_6=carteras_filtered.copy()
conditions_6= carteras_filtered_6['Portfolio'].isin(["LXMS0660","CL19778"])


STR006= carteras_filtered_6[conditions_6].dropna().copy()
STR006.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF006 = DF_filtered[['permId','str_006_sec']].copy()
DF006.columns =['permId','STR_006']
DF006['permId']=DF006['permId'].astype('int64')

DF006 =pd.merge(DF006,crossreference, on='permId', how='left')
DF006 =DF006 [['IssuerID','permId', 'IssuerName', 'STR_006']]

STR006 =pd.merge(STR006,DF006, on='IssuerID', how='left')
STR006= STR006.drop(columns=['permId','IssuerName_y']).drop_duplicates().dropna(subset=['STR_006'])

STR006= STR006.rename(columns={'IssuerName_x':'IssuerName','STR_006':'STR006'})
STR006= STR006[STR006['STR006']== 'EXCLUDED']
STR006= STR006[['IssuerID','IssuerName','Portfolio','STR006']]

# ## STR007

#STR007
carteras_filtered_7=carteras_filtered.copy()
conditions_7= carteras_filtered_7['Portfolio'].isin(["FIG05301"])


STR007= carteras_filtered_7[conditions_7].dropna().copy()
STR007.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF007 = DF_filtered[['permId','str_007_sect']].copy()
DF007.columns =['permId','STR_007']
DF007['permId']=DF007['permId'].astype('int64')

DF007 =pd.merge(DF007,crossreference, on='permId', how='left')
DF007 =DF007 [['IssuerID','permId', 'IssuerName', 'STR_007']]

STR007 =pd.merge(STR007,DF007, on='IssuerID', how='left')
STR007= STR007.drop(columns=['permId','IssuerName_y']).drop_duplicates().dropna(subset=['STR_007'])

STR007= STR007.rename(columns={'IssuerName_x':'IssuerName','STR_007':'STR007'})
STR007= STR007[STR007['STR007']== 'EXCLUDED']
STR007= STR007[['IssuerID','IssuerName','Portfolio','STR007']]

# ## CS001

#CS001
carteras_filtered_CS=carteras_filtered.copy()
conditions_CS= carteras_filtered_CS['Portfolio'].isin(["CPE03744"])


CS001= carteras_filtered_CS[conditions_CS].dropna().copy()
CS001.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DFCS001 = DF_filtered[['permId','cs_001_sec']].copy()
DFCS001.columns =['permId','CS_001']
DFCS001['permId']=DFCS001['permId'].astype('int64')

DFCS001 =pd.merge(DFCS001,crossreference, on='permId', how='left')
DFCS001 =DFCS001 [['IssuerID','permId', 'IssuerName', 'CS_001']]

CS001 =pd.merge(CS001,DFCS001, on='IssuerID', how='left')
CS001= CS001.drop(columns=['permId', 'IssuerName_y']).drop_duplicates().dropna(subset=['CS_001'])

CS001= CS001.rename(columns={'IssuerName_x':'IssuerName','CS_001':'CS001'})
CS001= CS001[CS001['CS001']== 'EXCLUDED']
CS001= CS001[['IssuerID','IssuerName','Portfolio','CS001']]

# ## ARTICULO 8 SFDR

#ARTICULO 8 SFDR
carteras_filtered_8=carteras_filtered.copy()
conditions_8= carteras_filtered_8['Portfolio'].isin(["FPG00015","FPG01421","FPH00254","EPV00052","EPV00051","EPV00050","FPG00026","FPG00555","EPV14006","FIF00154","FIF04284","FIF04285","FIG00026","FIG00689","FIG01973","FIG01988","FIG01998","FIG02164","FIG02410","FIG04251","FIG04253","FIG04868","FIG05178","FIG05198","FIG05415","FIG05428","FIG05698","FIH00058","FIH00208","FIH01197","FIH01920","FIN00315","FIN00441","FIN00637","CMD07211","CMD07212","CMD07213","CMD07214","CMD07215","CMD08211","CMD08212","CMD08213","CMD08214","CMD08215","PFIT0042","PFIT0516","PFIT0605","PFIT0647","PFIT1421","PFIT1422","PFIT1423","PFIT1424","PFIT1425","PFIT1426","PFIT1830","PFS00001","PFS00002","PFS00058","PFS00059","PFS00358","PFS00359","PFS00360","PFS00365","PFS00366","PFS00371","PFSULM01","PFSULM02","PFSULM03","PFSULM04","PFSULM05","LXMS0020","LXMS0040","LXMS0240","LXMS0360","LXMS0520","LXMS0530","LXMS0540","LXMS0340","LXMS0350","LXMS0650","LXMS0630","LXMS0550","LXMS0560","LXMS0570","PFI01549","PFIT0011","FPG00687","FPG01066","LXMS0580","FIG05774","LXMS0730"])

ART8= carteras_filtered_8[conditions_8].dropna().copy()
ART8.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DFART8 = DF_filtered[['permId','art_8_basicos']].copy()
DFART8.columns =['permId','ART_8']
DFART8['permId']=DFART8['permId'].astype('int64')

DFART8 =pd.merge(DFART8,crossreference, on='permId', how='left')
DFART8 =DFART8 [['IssuerID','permId', 'IssuerName', 'ART_8']]

ART8 =pd.merge(ART8,DFART8, on='IssuerID', how='left')
ART8= ART8.drop(columns=['permId','IssuerName_y']).drop_duplicates().dropna(subset=['ART_8'])

ART8= ART8.rename(columns={'IssuerName_x':'IssuerName','ART_8':'ART8'})
ART8= ART8[ART8['ART8']== 'EXCLUDED']
ART8= ART8[['IssuerID','IssuerName','Portfolio','ART8']]

# ## STR003B

#STR003B
conditions_3b= carteras_filtered['Portfolio'].isin(["CPE00035","CPE00169","CPE00264","CPE00277","CPE00363","CPE00375","CPE00448","CPE00456","CPE00464","CPE03002","CPE03290","CPE03519","CPE03570","CPE03853","CPE03867","CPE03912","CPE03913","CPE03928","CPE03986","CPE04036","CPE04037","CPE04039","CPE04041","CPE04044","CPE04053","CPE04059","CPE04063","CPE04070","CPE04071","CPE04075","CPE04076","CPE04077","CPE04080","CPE04081","CPE04082","CPE04084","CPE04087","CPE05134","CPE05137","CPE05148","CPE05150","CPE05151","CPE05152","CPE05162","CPE05174","CPE05177","CPE05180","CPE05189","CPE05190","CPE05191","CPE05192","CPE05207","CPE05208","CPE05211","CPE05212","CPE05218","CPE05219","CPE05226","CPE05234","CPE05239","CPE05242","CPE05244","CPE05245","CPE05248","CPE05252","CPE05258","CPE05261","CPE05314","CPE05315","CPE05317","CPE05318","CPE05337","CPE05340","CPE05341","CPE05344","CPE05351","CPE05355","CPE05361","CPE05362","CPE05363","CPE05366"])


STR003B= carteras_filtered[conditions_3b].dropna().copy()
STR003B.columns= ['IssuerName', 'IssuerID', 'Portfolio']

DF003B = DF_filtered[['permId','str_003b_ec']].copy()
DF003B.columns =['permId','STR_003B']
DF003B['permId']=DF003B['permId'].astype('int64')

DF003B =pd.merge(DF003B,crossreference, on='permId', how='left')

STR003B =pd.merge(STR003B,DF003B, on='IssuerID', how='left')
STR003B= STR003B.drop(columns=['permId', 'IssuerName_y']).drop_duplicates().dropna(subset=['STR_003B'])

STR003B= STR003B.rename(columns={'IssuerName_x':'IssuerName', 'STR_003B':'STR003B'})
STR003B= STR003B[STR003B['STR003B']== 'EXCLUDED']
STR003B= STR003B[['IssuerID', 'IssuerName','Portfolio','STR003B']]

# ## Fichero Incumplimientos

STR001.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False)
STR002.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
STR003.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
STR004.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
STR004_SB.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
STR005.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
STR006.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
STR007.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
CS001.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
ART8.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)
STR003B.to_csv("X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/04_Incumplimientos/Incumplimientos/Incumplimientos_022024.csv", sep=",", index=False, mode='a', header=True)