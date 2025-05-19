import pandas as pd
from datetime import datetime

DATE = datetime.now().strftime("%Y%m%d")
IDIR = r"C:\Users\n740789\Documents\Projects_local\datasets\datafeeds\raw_dataset\2025\20250401_Production\20250401_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
OFILE = rf"C:\Users\n740789\Documents\clarity_data_quality_controls\notebooks\output\{DATE}_inveractivo.xlsx"

TARGET_ISINS = [
    "FR0010307819",
    "DE0005190003",
    "DE000BASF111",
    "IT0004176001",
    "BE0974293251",
]

df = pd.read_csv(
    IDIR,
    dtype={"isin": str},
)

# Filter & Transpose
df_filtered = df[df["isin"].isin(TARGET_ISINS)].copy()
df_ft = df_filtered.T

# Save to csv without the index
df_ft.to_excel(
    OFILE,
    header=False,
)
