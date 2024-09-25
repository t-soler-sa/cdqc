import pandas as pd
from openpyxl import Workbook
import time

start_t = time.time()

# Reading the Excel file
BBDD_Overwrites = pd.read_excel("X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/BBDD_Overrides_may24.xlsx", sheet_name='MAPEO P-S')

# Renaming columns
BBDD_Overwrites.columns = ["#", "ClarityID", "PermID", "IssuerID", "IssuerName", "SNTWorld", "ParentNameAladdin", "ParentID", "ParSub", "ParentNameClarity", "ParentIdClarity", "GICS2", "GICS_2 Parent", "Sustainability Rating Parent", "OVR Inheritance BiC", "OVRSTR001", "OVRSTR002", "OVRSTR003", "OVRSTR003B", "OVRSTR004", "OVRSTR005", "OVRSTR006", "OVRSTR007", "OVRCS001", "OVRCS002", "OVRCS003", "OVRARTICULO8", "MotivoPrinc", "MotivoSec", "Detalle"]

# Filtering and renaming columns for each subset
def filter_and_rename(df, column_name, new_name):
    df_filtered = df[df[column_name].isin(["OK", "FLAG", "EXCLUDED"])]
    df_filtered = df_filtered[["ClarityID", column_name]].rename(columns={column_name: new_name})
    return df_filtered

OVRSTR001 = filter_and_rename(BBDD_Overwrites, "OVRSTR001", "STR001")
OVRSTR002 = filter_and_rename(BBDD_Overwrites, "OVRSTR002", "STR002")
OVRSTR003 = filter_and_rename(BBDD_Overwrites, "OVRSTR003", "STR003")
OVRSTR003B = filter_and_rename(BBDD_Overwrites, "OVRSTR003B", "STR003B")
OVRSTR004 = filter_and_rename(BBDD_Overwrites, "OVRSTR004", "STR004")
OVRSTR005 = filter_and_rename(BBDD_Overwrites, "OVRSTR005", "STR005")
OVRSTR006 = filter_and_rename(BBDD_Overwrites, "OVRSTR006", "STR006")
OVRSTR007 = filter_and_rename(BBDD_Overwrites, "OVRSTR007", "STR007")
OVRCS001 = filter_and_rename(BBDD_Overwrites, "OVRCS001", "CS001")
OVRCS002 = filter_and_rename(BBDD_Overwrites, "OVRCS002", "CS002")
OVRCS003 = filter_and_rename(BBDD_Overwrites, "OVRCS003", "CS003")
OVRARTICULO8 = filter_and_rename(BBDD_Overwrites, "OVRARTICULO8", "ARTICULO 8")

# Writing to Excel files
def write_to_excel(df, path):
    df.to_excel(path, index=False)

write_to_excel(OVRSTR001, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_001_SEC_202405.xlsx")
write_to_excel(OVRSTR002, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_002_SEC_202405.xlsx")
write_to_excel(OVRSTR003, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_003_SEC_202405.xlsx")
write_to_excel(OVRSTR003B, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_003B_EC_202405.xlsx")
write_to_excel(OVRSTR004, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_004_SEC_202405.xlsx")
write_to_excel(OVRSTR005, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_005_SEC_202405.xlsx")
write_to_excel(OVRSTR006, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_006_SEC_202405.xlsx")
write_to_excel(OVRSTR007, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_007_SECT_202405.xlsx")
write_to_excel(OVRCS001, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/CS_001_SEC_202405.xlsx")
write_to_excel(OVRCS002, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/CS_002_EC_202405.xlsx")
write_to_excel(OVRCS003, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/CS_003_SEC_202405.xlsx")
write_to_excel(OVRARTICULO8, "X:/INVDESPRO/INVESTIGACION/Fondos Éticos/3. DATASETS/04_Datos Clarity/01_Equities_feed/03_Overwrites/202405_OVR_May/STR_SFDR8_AEC_202405.xlsx")


end = time.time()
print(f"{end - start_t:.2f} seconds")