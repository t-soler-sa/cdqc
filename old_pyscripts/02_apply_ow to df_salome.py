import numpy as np
import pandas as pd

#DATA FEED CLARITY, actual,copiar ruta del mes en curso 

#TODO DEBE ESTAR EN '/' 

ruta_DF = 'X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/01_Ficheros_originales (Descarga en bruto)/20240201_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv'

#crossreference del mes 

crossreference=pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/03_Cross Reference/20240201_Aladdin_Clarity_Issuers.xlsx')

#Carteras, sustituir por cartera a utilizar --> `C:/Users/nXXXXXX/Downloads/xxxxxx.xlsx`. Descargar de Aladdin (Workspace: Descarga_Incumplimientos)
file_path= 'C:/Users/n629352/Downloads/Risk and exposure - 2024-02-13T101801.182.xlsx'
Carteras =pd.read_excel(file_path, sheet_name =None, skiprows=4)

Carteras= pd.concat(Carteras.values(), ignore_index=True)

# # 3

#OW del mes en curso, desde ruta #TODO DEBE ESTAR EN '/'
#Data Frame de cada carga de OW con renombre, acceso desde ruta

#Read 1 OW
CS_001_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/CS_001_SEC_202402.xlsx')

#Read 2 OW
CS_002_OW =pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/CS_002_EC_202402.xlsx')

#Read 3 OW
CS_003_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/CS_003_SEC_202402.xlsx')

#Read 4 OW
STR_001_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_001_SEC_202402.xlsx')

#Read 5 OW
STR_002_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_002_SEC_202402.xlsx')

#Read 6 OW
STR_003_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_003_SEC_202402.xlsx')

#Read 7 OW
STR_004_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_004_SEC_202402.xlsx')

#Read 8 OW
STR_005_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_005_SEC_202402.xlsx')

#Read 9 OW
STR_006_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_006_SEC_202402.xlsx')

#Read 10 OW
STR_007_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_007_SECT_202402.xlsx')

#Read 11 OW
STR_SFDR8_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_SFDR8_AEC_202402.xlsx')

#Read 12 OW
STR_003B_OW = pd.read_excel('X:/INVDESPRO/INVESTIGACION/Fondos éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202402_OVR_Feb/STR_003B_EC_202402.xlsx')

# ### DATA FEED

#DATA FEED CLARITY, actual
DF = pd.read_csv(ruta_DF, sep=',', dtype='unicode')

#DF.columns Ubicacación de cada columna a verificar, que se van a cambiar por la validación
##array([242, 245, 244, 236, 237, 238, 239, 241, 246, 247, 251, 254], dtype=int64) este es el resultado esperado 

DF.columns.get_indexer(['cs_001_sec','cs_002_ec','cs_003_sec','str_001_s','str_002_ec','str_003_ec','str_004_asec','str_005_ec','str_006_sec','str_007_sect','art_8_basicos','str_003b_ec'])

#Pasar la etiqueta 'ClarityID' a int
#DF['ClarityID'] = DF['ClarityID'].fillna(0).astype(int)
DF['ClarityID'] = DF['ClarityID'].astype('int64')
DF['ClarityID'].info()

# ## OW del mes  en curso

# ## OW_1

#Col a utilizar del fichero de Clarity para comparar con el primer OW
CS_001_DF= DF[['ClarityID','cs_001_sec']]

#Union por ID
OW_1 = pd.merge(CS_001_DF,CS_001_OW, how='left', on='ClarityID')

#OW_1 columna de verificación = 'CS001_New' (NaN)
OW_1['CS001_New']= np.where((OW_1['CS001']!= OW_1['cs_001_sec']),OW_1['CS001'],OW_1['cs_001_sec']) 

#Para rellenar los NaN en 'CS001_New' con valores ='cs_001_sec' 
OW_1['CS001_New'] = OW_1['CS001_New'].fillna(OW_1['cs_001_sec'])

#Siguiente dejar solo CS001_New, esta col al nuevo DF luego renombrar
OW_1.drop(['ClarityID','cs_001_sec', 'CS001'], axis = 'columns', inplace=True)

DF.insert(242,'CS001_Name',OW_1['CS001_New']) ##Insertar verificación
DF.drop(['cs_001_sec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('CS001_Name') #Verificar ubicación correcta ##OK (242)

# ### OW_ 2

#Col a utilizar del fichero de Clarity para comparar con el 2do OW
CS_002_DF= DF[['ClarityID','cs_002_ec']]
#Union por ID
OW_2 = pd.merge(left=CS_002_DF,right=CS_002_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_2 columna de veirifcación = 'CS002_New' (NaN) 'CS002_New' NaN
OW_2['CS002_New']= np.where((OW_2['CS002']!= OW_2['cs_002_ec']),OW_2['CS002'],OW_2['cs_002_ec']) 
#Para rellenar los NaN en 'CS002_New' con valores ='cs_002_ec' 
OW_2['CS002_New'] = OW_2['CS002_New'].fillna(OW_2['cs_002_ec'])
#Siguiente dejar solo 'CS002_New', esta col al nuevo DF luego renombrar
OW_2.drop(['ClarityID','cs_002_ec', 'CS002'], axis = 'columns', inplace=True)

DF.columns.get_loc('cs_002_ec') #Verificar ubicación correcta (245)

DF.insert(245,'CS002_Name',OW_2['CS002_New']) ##Insertar verificación
DF.drop(['cs_002_ec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('CS002_Name') #Verificar ubicación correcta ##OK (245)

# ### OW_ 3

#Col a utilizar del fichero de Clarity para comparar con el 3er OW
CS_003_DF= DF[['ClarityID','cs_003_sec']]
#Union por ID
OW_3 = pd.merge(left=CS_003_DF,right=CS_003_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_3 con 'CS003_New' NaN
OW_3['CS003_New']= np.where((OW_3['CS003']!= OW_3['cs_003_sec']),OW_3['CS003'],OW_3['cs_003_sec']) 
#Para rellenar los NaN de 'CS003_New' con 'cs_003_sec'
OW_3['CS003_New'] = OW_3['CS003_New'].fillna(OW_3['cs_003_sec'])
#Siguiente dejar solo CS003_New
OW_3.drop(['ClarityID','cs_003_sec', 'CS003'], axis = 'columns', inplace=True)

DF.columns.get_loc('cs_003_sec') #Verificar ubicación correcta  en el DF original #OK (244)

DF.insert(244,'CS003_Name',OW_3['CS003_New']) ##Insertar verificación
DF.drop(['cs_003_sec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('CS003_Name') #Verificar ubicación correcta ##OK (244)


# ## OW_4


#Col a utilizar del fichero de Clarity OW
STR_001_DF= DF[['ClarityID','str_001_s']]
#Union por ID
OW_4 = pd.merge(left=STR_001_DF,right=STR_001_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_4 con 'str_001_s_New' NaN
OW_4['str_001_s_New']= np.where((OW_4['STR001']!= OW_4['str_001_s']),OW_4['STR001'],OW_4['str_001_s']) 
#Para rellenar los NaN en 'str_001_s_New'
OW_4['str_001_s_New'] = OW_4['str_001_s_New'].fillna(OW_4['str_001_s'])
#Siguiente dejar solo 'str_001_s_New'
OW_4.drop(['ClarityID','str_001_s', 'STR001'], axis = 'columns', inplace=True)
OW_4.info()


DF.columns.get_loc('str_001_s') #Verificar ubicación correcta ##OK (236)


DF.insert(236,'STR001_Name',OW_4['str_001_s_New']) #Insertar verificación
DF.drop(['str_001_s'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR001_Name') #Verificar ubicación correcta ##OK (236)

# ## OW_5

#Col a utilizar del fichero de Clarity OW
STR_002_DF= DF[['ClarityID','str_002_ec']]
#Union por ID
OW_5 = pd.merge(left=STR_002_DF,right=STR_002_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_5 con 'str_002_ec_New' NaN
OW_5['str_002_ec_New']= np.where((OW_5['STR002']!= OW_5['str_002_ec']),OW_5['STR002'],OW_5['str_002_ec']) 
#Para rellenar los NaN en 'str_002_ec_New'
OW_5['str_002_ec_New'] = OW_5['str_002_ec_New'].fillna(OW_5['str_002_ec'])
#Siguiente dejar solo ''str_002_ec_New''
OW_5.drop(['ClarityID','str_002_ec', 'STR002'], axis = 'columns', inplace=True)
OW_5.info()

DF.columns.get_loc('str_002_ec') #Verificar ubicación correcta ##OK (237)

DF.insert(237,'STR002_Name',OW_5['str_002_ec_New']) #Insertar verificación
DF.drop(['str_002_ec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR002_Name') #Verificar ubicación correcta ##OK (237)

# ## OW_6

#Col a utilizar del fichero de Clarity OW
STR_003_DF= DF[['ClarityID','str_003_ec']]
#Union por ID
OW_6 = pd.merge(left=STR_003_DF,right=STR_003_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_6 con 'str_003_ec_New' NaN
OW_6['str_003_ec_New']= np.where((OW_6['STR003']!= OW_6['str_003_ec']),OW_6['STR003'],OW_6['str_003_ec']) 
#Para rellenar los NaN en 'str_003_ec_New'
OW_6['str_003_ec_New'] = OW_6['str_003_ec_New'].fillna(OW_6['str_003_ec'])
#Siguiente dejar solo ''str_003_ec_New''
OW_6.drop(['ClarityID','str_003_ec', 'STR003'], axis = 'columns', inplace=True)
OW_6.info()

DF.columns.get_loc('str_003_ec') #Verificar ubicación correcta ##OK (238)

DF.insert(238,'STR003_Name',OW_6['str_003_ec_New']) #Insertar verificación
DF.drop(['str_003_ec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR003_Name') #Verificar ubicación correcta ##OK (238)

# ## OW_7

#Col a utilizar del fichero de Clarity OW
STR_004_DF= DF[['ClarityID','str_004_asec']]
#Union por ID
OW_7 = pd.merge(left=STR_004_DF,right=STR_004_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_7 con 'str_003_ec_New' NaN
OW_7['str_004_asec_New']= np.where((OW_7['STR004']!= OW_7['str_004_asec']),OW_7['STR004'],OW_7['str_004_asec']) 
#Para rellenar los NaN en ''str_004_asec_New''
OW_7['str_004_asec_New'] = OW_7['str_004_asec_New'].fillna(OW_7['str_004_asec'])
#Siguiente dejar solo ''str_004_asec_New''
OW_7.drop(['ClarityID','str_004_asec', 'STR004'], axis = 'columns', inplace=True)

DF.columns.get_loc('str_004_asec') #Verificar ubicación correcta ##OK (239)

DF.insert(239,'STR004_Name',OW_7['str_004_asec_New']) #Insertar verificación
DF.drop(['str_004_asec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR004_Name') #Verificar ubicación correcta ##OK (239)

# ## OW_8

#Col a utilizar del fichero de Clarity OW
STR_005_DF= DF[['ClarityID','str_005_ec']]
#Union por ID
OW_8 = pd.merge(left=STR_005_DF,right=STR_005_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_8 con 'str_005_ec_New' NaN
OW_8['str_005_ec_New']= np.where((OW_8['STR005']!= OW_8['str_005_ec']),OW_8['STR005'],OW_8['str_005_ec']) 
#Para rellenar los NaN en 'str_005_ec_New''
OW_8['str_005_ec_New'] = OW_8['str_005_ec_New'].fillna(OW_8['str_005_ec'])
#Siguiente dejar solo ''str_005_ec_New''
OW_8.drop(['ClarityID','str_005_ec', 'STR005'], axis = 'columns', inplace=True)

DF.columns.get_loc('str_005_ec') #Verificar ubicación correcta ##OK (241)


DF.insert(241,'STR005_Name',OW_8['str_005_ec_New']) #Insertar verificación
DF.drop(['str_005_ec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR005_Name') #Verificar ubicación correcta ##OK (241)


# ## OW_9


#Col a utilizar del fichero de Clarity OW
STR_006_DF= DF[['ClarityID','str_006_sec']]
#Union por ID
OW_9 = pd.merge(left=STR_006_DF,right=STR_006_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_9 con 'str_005_ec_New' NaN
OW_9['str_006_sec_New']= np.where((OW_9['STR006']!= OW_9['str_006_sec']),OW_9['STR006'],OW_9['str_006_sec']) 
#Para rellenar los NaN en 'str_006_sec_New'
OW_9['str_006_sec_New'] = OW_9['str_006_sec_New'].fillna(OW_9['str_006_sec'])
#Siguiente dejar solo 'str_006_sec_New'
OW_9.drop(['ClarityID','str_006_sec', 'STR006'], axis = 'columns', inplace=True)

DF.columns.get_loc('str_006_sec') #Verificar ubicación correcta ##OK (246)


DF.insert(246,'STR006_Name',OW_9['str_006_sec_New'])#Insertar verificación
DF.drop(['str_006_sec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR006_Name') #Verificar ubicación correcta ##OK (246)DF.columns.get_loc('STR006_Name') 


# ## OW_10


#Col a utilizar del fichero de Clarity OW
STR_007_DF= DF[['ClarityID','str_007_sect']]
#Union por ID
OW_10 = pd.merge(left=STR_007_DF,right=STR_007_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_10 con 'str_007_sect_New' NaN
OW_10['str_007_sect_New']= np.where((OW_10['STR007']!= OW_10['str_007_sect']),OW_10['STR007'],OW_10['str_007_sect']) 
#Para rellenar los NaN en 'str_007_sect_New'
OW_10['str_007_sect_New'] = OW_10['str_007_sect_New'].fillna(OW_10['str_007_sect'])
#Siguiente dejar solo 'str_006_sec_New'
OW_10.drop(['ClarityID','str_007_sect', 'STR007'], axis = 'columns', inplace=True)

DF.columns.get_loc('str_007_sect') #Verificar ubicación correcta ##OK (247)

DF.insert(247,'STR007_Name',OW_10['str_007_sect_New']) #Insertar verificación
DF.drop(['str_007_sect'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR007_Name') #Verificar ubicación correcta ##OK (247)

# ## OW_11

#Col a utilizar del fichero de Clarity OW
STR_SFDR8_DF= DF[['ClarityID','art_8_basicos']]
#Union por ID
OW_11 = pd.merge(left=STR_SFDR8_DF,right=STR_SFDR8_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_11 con 'art_8_basicos_New' NaN
OW_11['art_8_basicos_New']= np.where((OW_11['ARTICULO 8']!= OW_11['art_8_basicos']),OW_11['ARTICULO 8'],OW_11['art_8_basicos']) 
#Para rellenar los NaN en 'art_8_basicos_New'
OW_11['art_8_basicos_New'] = OW_11['art_8_basicos_New'].fillna(OW_11['art_8_basicos'])
#Siguiente dejar solo 'sart_8_basicos_New'
OW_11.drop(['ClarityID','art_8_basicos', 'ARTICULO 8'], axis = 'columns', inplace=True)
OW_11.info()

DF.columns.get_loc('art_8_basicos') #Verificar ubicación correcta ##OK (251)

DF.insert(251,'ARTICULO8_Name',OW_11['art_8_basicos_New'])  #Insertar verificación
DF.drop(['art_8_basicos'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('ARTICULO8_Name') #Verificar ubicación correcta ##OK (251)

# ## OW_12

#Col a utilizar del fichero de Clarity OW
STR_003B_DF= DF[['ClarityID','str_003b_ec']]
#Union por ID
OW_12 = pd.merge(left=STR_003B_DF,right=STR_003B_OW, how='left', left_on='ClarityID', right_on='ClarityID')
#OW_12 con 'str_003b_ec_New' NaN
OW_12['str_003b_ec_New']= np.where((OW_12['STR003B']!= OW_12['str_003b_ec']),OW_12['STR003B'],OW_12['str_003b_ec']) 
#Para rellenar los NaN en 'str_003b_ec_New'
OW_12['str_003b_ec_New'] = OW_12['str_003b_ec_New'].fillna(OW_12['str_003b_ec'])
#Siguiente dejar solo 'str_003b_ec_New'
OW_12.drop(['ClarityID','str_003b_ec', 'STR003B'], axis = 'columns', inplace=True)
OW_12.info()

DF.columns.get_loc('str_003b_ec') #Verificar ubicación correcta ##OK (254)

DF.insert(254,'STR003B_Name',OW_12['str_003b_ec_New'])  #Insertar verificación
DF.drop(['str_003b_ec'], axis = 'columns', inplace=True) #Elimino la original
DF.columns.get_loc('STR003B_Name') #Verificar ubicación correcta ##OK (254)

# ### DF

DF.shape #Comprobación de filas y columnas

#([242, 245, 244, 236, 237, 238, 239, 241, 246, 247, 251, 254], dtype=int64) este tiene que ser el resultado 
DF.columns.get_indexer(['CS001_Name', 'CS002_Name', 'CS003_Name', 'STR001_Name', 'STR002_Name','STR003_Name','STR004_Name','STR005_Name','STR006_Name','STR007_Name','ARTICULO8_Name','STR003B_Name'])

#Renombrar la verificación por nombre original DF
DF.rename(columns = {'CS001_Name':'cs_001_sec', 'CS002_Name':'cs_002_ec', 'CS003_Name':'cs_003_sec', 'STR001_Name':'str_001_s', 'STR002_Name':'str_002_ec','STR003_Name':'str_003_ec','STR004_Name':'str_004_asec','STR005_Name':'str_005_ec','STR006_Name':'str_006_sec','STR007_Name':'str_007_sect','ARTICULO8_Name':'art_8_basicos','STR003B_Name':'str_003b_ec'}, inplace =True)
#Verificación loc renombradas
DF.columns.get_indexer(['cs_001_sec','cs_002_ec','cs_003_sec','str_001_s','str_002_ec','str_003_ec','str_004_asec','str_005_ec','str_006_sec','str_007_sect','art_8_basicos','str_003b_ec'])

# ## Nuevo Data Feed aplicados OW

# # 4

#NUEVO CSV, indicar nuevo nombre y que deberia ser siempre, sobre escribir , agregarle,  mode='w',sino se va a cambiar el nombre 

#Cuando se ejecute se le debe cambiar el nombre con el mes en curso, según formato de los anteriores

DF.to_csv(r'X:\INVDESPRO\INVESTIGACION\Fondos éticos\3. DATASETS\04_Datos Clarity\01_Equities_feed\DF_para_python\202402_DF_Febrero.csv', sep=';', header=True, index=False)