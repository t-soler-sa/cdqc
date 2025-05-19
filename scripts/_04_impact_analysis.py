import argparse
import logging
import os
import time
import warnings
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path

import pandas as pd


from scripts.utils.dataloaders import (
    load_clarity_data,
    load_crossreference,
)
from scripts.utils.config import get_config

import sys

# print(sys.path)
# print(">>> Running the REAL _02_apply_ovr.py from:", __file__)


# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 0. CONFIGURATION & I/O PATHS
# Get the common configuration for the generator of override list for SAM BAU Infinity
config = get_config("04-impact-analysis", interactive=False, gen_output_dir=False)
logger = config["logger"]
DATE = config["DATE"]
paths = config["paths"]
CROSSREFERENCE_PATH = paths["CROSSREFERENCE_PATH"]

# Define base directories
base_dir = Path("C:/Users/n740789/Documents/Projects_local/DataSets")
datafeed_dir = base_dir / "datafeeds/datafeeds_with_ovr"
datafeed_path = datafeed_dir / f"{DATE}_df_issuer_level_with_ovr.csv"

datafeed_columns = [
    "permid",
    "art_8_basicos",
    "sustainability_rating",
    "str_001_s",
    "str_004_asec",
    "str_007_sect",
]


# add remove warnign for openpyxl
@contextmanager  # This is a context manager that suppresses the warning
def suppress_openpyxl_warning():
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message="Workbook contains no default style, apply openpyxl's default",
        )
        yield


# Define a function to reorder columns moving to the end columns starting with "str_", "art_" and "sustainability_"
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


def analysis(
    input_file: str,
    output_file: str,
    datafeed_col: list,
    date: str,
    datafeed: pd.DataFrame,
    crossreference: pd.DataFrame,
):
    logger.info(f"\n\nGenerating Impact Analysis for {input_file}")
    df = datafeed[datafeed_col].copy()
    # read aladdin workbench excel file
    logger.info("Loading Aladdin Workbench file")
    with suppress_openpyxl_warning():
        portfolio = pd.read_excel(input_file, sheet_name="Portfolio", skiprows=3)
        benchmark = pd.read_excel(input_file, sheet_name="Benchmark", skiprows=3)

    portfolio.rename(columns={"Issuer ID": "aladdin_id"}, inplace=True)
    benchmark.rename(columns={"Issuer ID": "aladdin_id"}, inplace=True)
    # convert aladdin_id to string
    portfolio["aladdin_id"] = portfolio["aladdin_id"].astype(str)
    benchmark["aladdin_id"] = benchmark["aladdin_id"].astype(str)

    logger.info("Aladdin Workbench file loaded")

    # PROCESS DATASETS
    # add permid to portfolio and benchmark from crossreference
    logger.info("adding permid to portfolios and benchmarks")
    portfolio = pd.merge(
        portfolio, crossreference[["aladdin_id", "permid"]], how="left", on="aladdin_id"
    )
    benchmark = pd.merge(
        benchmark, crossreference[["aladdin_id", "permid"]], how="left", on="aladdin_id"
    )
    logger.info("permid added successfully")

    # add datafeed columns to portfolio and benchmark with suffixes "_current" and "_new"
    logger.info(f"adding datafeed columns to portfolio and benchmark")
    portfolio = pd.merge(
        portfolio, df, how="left", on="aladdin_id", suffixes=("_current", "_new")
    )
    benchmark = pd.merge(
        benchmark, df, how="left", on="aladdin_id", suffixes=("_current", "_new")
    )
    logger.info("datafeed columns added")

    # reordering columns
    logger.info("reordering columns")
    portfolio = reorder_columns(portfolio)
    benchmark = reorder_columns(benchmark)
    logger.info("columns sorted")

    for df_name, df in zip(["portfolio", "bencmark"], [portfolio, benchmark]):
        len_permid_current = df["permid_current"].notna().sum()
        len_permid_new = df["permid_new"].notna().sum()

        if len_permid_current != len_permid_new:
            logger.warning(
                f"For {df_name} the number of permid_current ({len_permid_current}) != number of permid_new ({len_permid_new})"
            )
            # Determine base column name
            base_col = "permid"
            col_current = f"{base_col}_current"
            col_new = f"{base_col}_new"

            # Combine the two columns into one, preferring '_current' if it exists
            df[base_col] = df[col_current].combine_first(df[col_new])

            # Drop the original two columns
            df.drop(columns=[col_current, col_new], inplace=True)

        else:
            df.rename(columns={"permid_current": "permid"}, inplace=True)
            df.drop(columns=["permid_new"], inplace=True)

    # SAVE DATASETS TO EXCEL FILE
    logger.info("Saving dataframe to Excel")
    with suppress_openpyxl_warning():
        with pd.ExcelWriter(output_file) as writer:
            portfolio.to_excel(writer, sheet_name="portfolio", index=False)
            benchmark.to_excel(writer, sheet_name="benchmark", index=False)
    logger.info(f"Impact Analysis: Results saved to excel on {output_file}")
    del df


def process_directory(
    input_dir: str,
    output_dir: str,
    datafeed_col: list,
    date: str,
    datafeed: pd.DataFrame,
    crossreference: pd.DataFrame,
):
    for file in os.listdir(input_dir):
        if file.endswith(".xlsx"):
            if any(
                portfolio_id in file
                for portfolio_id in ["FPB01158", "FPH00457", "EPH00107"]
            ):
                logger.info(f"Processing {file} with str 004 instead of str 007")
                datafeed_col = [
                    "permid",
                    "aladdin_id",
                    "sustainability_rating",
                    "str_004_asec",
                ]
                input_file = os.path.join(input_dir, file)
                output_file = os.path.join(
                    output_dir, file.replace(".xlsx", "_analysis.xlsx")
                )
                analysis(
                    input_file,
                    output_file,
                    datafeed_col,
                    date,
                    datafeed,
                    crossreference,
                )
            else:
                input_file = os.path.join(input_dir, file)
                output_file = os.path.join(
                    output_dir, file.replace(".xlsx", "_analysis.xlsx")
                )
                analysis(
                    input_file,
                    output_file,
                    datafeed_col,
                    date,
                    datafeed,
                    crossreference,
                )


def main():
    date = DATE
    yymm = date[2:]  # Extract last 4 digits for YYMM format
    base_dir = f"C:\\Users\\n740789\\Documents\\Projects_local\\DataSets\\impact_analysis\\{yymm}"
    input_base = os.path.join(base_dir, "aladdin_input")
    output_base = os.path.join(base_dir, "analysis_output")

    logger.info("Loading crossreference")
    crossreference = load_crossreference(CROSSREFERENCE_PATH)
    # remove duplicate and nan permid in crossreference
    logger.info("Removing duplicates and NaN values from crossreference")
    crossreference.drop_duplicates(subset=["permid"], inplace=True)
    crossreference.dropna(subset=["permid"], inplace=True)
    # convert permid and aladdin to string
    crossreference["permid"] = crossreference["permid"].astype(str)
    crossreference["aladdin_id"] = crossreference["aladdin_id"].astype(str)
    # LOAD DATASETS & MODIFY COLUMN NAMES
    datafeed = load_clarity_data(datafeed_path, target_cols=datafeed_columns)
    # add aladdin_id to datafeed from crossreference
    datafeed = datafeed.merge(
        crossreference[["permid", "aladdin_id"]], on="permid", how="left"
    )

    # Ensure output directories exist
    for dir_name in ["art8_analysis", "esg_analysis", "sustainable_analysis"]:
        os.makedirs(os.path.join(output_base, dir_name), exist_ok=True)

    # Process Art 8 Basico
    process_directory(
        os.path.join(input_base, "art_8_basico"),
        os.path.join(output_base, "art8_analysis"),
        ["permid", "aladdin_id", "art_8_basicos"],
        date,
        datafeed=datafeed,
        crossreference=crossreference,
    )

    # Process ESG
    process_directory(
        os.path.join(input_base, "ESG"),
        os.path.join(output_base, "esg_analysis"),
        ["permid", "aladdin_id", "sustainability_rating", "str_001_s"],
        date,
        datafeed=datafeed,
        crossreference=crossreference,
    )

    # Process Sustainable
    process_directory(
        os.path.join(input_base, "Sustainable"),
        os.path.join(output_base, "sustainable_analysis"),
        ["permid", "aladdin_id", "sustainability_rating", "str_007_sect"],
        date,
        datafeed=datafeed,
        crossreference=crossreference,
    )

    logger.info("Script completed")


if __name__ == "__main__":
    main()
