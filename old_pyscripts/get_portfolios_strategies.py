import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

date = datetime.now()
date_str = date.strftime("%y%m")

# Define paths and constants
REPO_DIR = Path(__file__).resolve().parents[1]
COMMITTEE_PATH = REPO_DIR / "excel_books" / "sri_data" / "committee_portfolios"
INPUT_PATH = COMMITTEE_PATH / "202502_Inventario Reglas Compliance productos ISR.xlsx"
OUTPU_FILE_NAME = f"{date_str}_strategies_portfolios_ids.csv"
OUTPUT_PATH = COMMITTEE_PATH / OUTPU_FILE_NAME


# Define transformation function
def transform_csv(input_path: str = INPUT_PATH) -> pd.DataFrame:
    target_columns = ["Estrategia/columna Aladdin", "Código Aladdin"]

    if target_columns is None:
        raise ValueError(f"Target columns {target_columns} not found")
        exit

    # 1) read csv & target columns
    logging.info(f"Reading file: {input_path}")
    df = pd.read_excel(
        input_path,
        skiprows=1,
        sheet_name="Productos SRI",
        usecols=target_columns,
    )

    # 2) rename target colums
    df.rename(
        columns={
            "Estrategia/columna Aladdin": "strategy",
            "Código Aladdin": "portfolio_id",
        },
        inplace=True,
    )

    # 3) Group by "strategy" and collect all portfolio_names in a list
    grouped = df.groupby("strategy")["portfolio_id"].apply(list)

    # 4) Convert the grouped object to a dictionary
    grouped_dict = grouped.to_dict()

    # 5) Create a DataFrame with each strategy as a row; transpose to make each strategy a column
    df_out = pd.DataFrame.from_dict(grouped_dict, orient="index").transpose()

    # 6) Optionally fill missing values with empty string (instead of NaN)
    df_out.fillna("", inplace=True)

    return df_out


def main():
    logging.info(f"Filtering portfolios and strategies")
    portfolio_strat = transform_csv()

    logging.info(f"Saving file: {OUTPUT_PATH}")
    portfolio_strat.to_csv(OUTPUT_PATH, index=None, encoding="utf-8")


if __name__ == "__main__":
    main()
