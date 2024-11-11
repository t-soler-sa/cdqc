import logging
import os
import time
import warnings
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
import argparse
from datetime import datetime

import pandas as pd


# add remove warnign for openpyxl
@contextmanager
def suppress_openpyxl_warning():
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message="Workbook contains no default style, apply openpyxl's default",
        )
        yield


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"Total execution time: {execution_time:.2f} seconds")
        return result

    return wrapper


# Define a function to validate the date format
def validate_date(date_string):
    try:
        datetime.strptime(date_string, "%Y%m")
        return True
    except ValueError:
        return False


# Define a function to get the date from user input
def get_date():
    parser = argparse.ArgumentParser(description="Process data for a specific date.")
    parser.add_argument("date", nargs="?", help="Date in YYYYMM format")
    parser.add_argument("--date", dest="date_flag", help="Date in YYYYMM format")
    args = parser.parse_args()

    if args.date and validate_date(args.date):
        return args.date
    elif args.date_flag and validate_date(args.date_flag):
        return args.date_flag
    else:
        while True:
            date_input = input("Enter the date in YYYYMM format: ")
            if validate_date(date_input):
                return date_input
            print("Invalid date format. Please use YYYYMM.")


def reorder_columns(df):
    cols = df.columns.tolist()
    start_cols = [
        col
        for col in cols
        if not (
            col.startswith("str_")
            or col.startswith("art_")
            or col.startswith("sustainability_")
        )
    ]
    end_cols = [
        col
        for col in cols
        if col.startswith("str_")
        or col.startswith("art_")
        or col.startswith("sustainability_")
    ]
    end_cols.sort()
    new_order = start_cols + end_cols
    return df[new_order]


def analysis(input_file: str, output_file: str, datafeed_col: list, date: str):
    logging.info(f"Generating Impact Analysis for {input_file}")

    # READ DATASETS
    # LOAD DATASETS & MODIFY COLUMN NAMES
    logging.info("Loading crossreference")
    cross = pd.read_csv(
        f"C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\crossreference\\Aladdin_Clarity_Issuers_{date}01.csv",
        dtype={"CLARITY_AI": str},
    )
    cross.rename(
        columns={"Aladdin_Issuer": "issuer_id", "CLARITY_AI": "permid"}, inplace=True
    )
    logging.info("Crossreference loaded")

    # read datafeed
    logging.info("Loading datafeed")
    df = pd.read_csv(
        f"C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\DATAFEED\\datafeeds_with_ovr\\{date}_df_issuer_level_with_ovr.csv",
        usecols=datafeed_col,
        dtype=str,
    )
    logging.info("Datafeed loaded")

    # read aladdin workbench excel file
    logging.info("Loading Aladdin Workbench file")
    with suppress_openpyxl_warning():
        portfolio = pd.read_excel(input_file, sheet_name="Portfolio", skiprows=3)
        benchmark = pd.read_excel(input_file, sheet_name="Benchmark", skiprows=3)

    portfolio.rename(columns={"Issuer ID": "issuer_id"}, inplace=True)
    benchmark.rename(columns={"Issuer ID": "issuer_id"}, inplace=True)
    logging.info("Aladdin Workbench file loaded")

    # PROCESS DATASETS
    logging.info("adding permid to portfolios and benchmarks")
    portfolio = pd.merge(
        portfolio, cross[["issuer_id", "permid"]], how="left", on="issuer_id"
    )
    benchmark = pd.merge(
        benchmark, cross[["issuer_id", "permid"]], how="left", on="issuer_id"
    )
    logging.info("permid added successfully")

    logging.info(f"adding datafeed columns to portfolio and benchmark")
    portfolio = pd.merge(
        portfolio, df, how="left", on="permid", suffixes=("_current", "_new")
    )
    benchmark = pd.merge(
        benchmark, df, how="left", on="permid", suffixes=("_current", "_new")
    )
    logging.info("datafeed columns added")

    logging.info("reordering columns")
    portfolio = reorder_columns(portfolio)
    benchmark = reorder_columns(benchmark)
    logging.info("columns sorted")

    # SAVE DATASETS TO EXCEL FILE
    logging.info("Saving dataframe to Excel")
    with suppress_openpyxl_warning():
        with pd.ExcelWriter(output_file) as writer:
            portfolio.to_excel(writer, sheet_name="portfolio", index=False)
            benchmark.to_excel(writer, sheet_name="benchmark", index=False)
    logging.info(f"Impact Analysis: Results saved to excel on {output_file}")


def process_directory(input_dir: str, output_dir: str, datafeed_col: list, date: str):
    for file in os.listdir(input_dir):
        if file.endswith(".xlsx"):
            input_file = os.path.join(input_dir, file)
            output_file = os.path.join(
                output_dir, file.replace(".xlsx", "_analysis.xlsx")
            )
            analysis(input_file, output_file, datafeed_col, date)


@measure_time
def main():
    setup_logging()
    date = get_date()
    yymm = date[2:]  # Extract last 4 digits for YYMM format
    base_dir = f"C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\impact_analysis\\{yymm}"
    input_base = os.path.join(base_dir, "aladdin_input")
    output_base = os.path.join(base_dir, "analysis_output")

    # Ensure output directories exist
    for dir_name in ["art8_analysis", "esg_analysis", "sustainable_analysis"]:
        os.makedirs(os.path.join(output_base, dir_name), exist_ok=True)

    # Process Art 8 Basico
    process_directory(
        os.path.join(input_base, "art_8_basico"),
        os.path.join(output_base, "art8_analysis"),
        ["permid", "art_8_basicos"],
        date,
    )

    # Process ESG
    process_directory(
        os.path.join(input_base, "ESG"),
        os.path.join(output_base, "esg_analysis"),
        ["permid", "sustainability_rating", "str_001_s"],
        date,
    )

    # Process Sustainable
    process_directory(
        os.path.join(input_base, "Sustainable"),
        os.path.join(output_base, "sustainable_analysis"),
        ["permid", "sustainability_rating", "str_004_asec"],
        date,
    )

    logging.info("Script completed")


if __name__ == "__main__":
    main()
