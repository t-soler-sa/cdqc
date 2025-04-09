import argparse
import logging
import os
import time
import warnings
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, List, Optional

import pandas as pd

"""
========== DEFINE AUX FUNCTIONS ==========
"""

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Ignore workbook warnings
@contextmanager
def suppress_openpyxl_warning():
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
        yield


# Timer context manager
@contextmanager
def timer(description: str):
    start = time.time()
    yield
    elapsed_time = time.time() - start
    logging.info(f"{description} took {elapsed_time:.2f} seconds")


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


# Define a function to load data from csv and excel files
def load_data(
    filepath: Path, target_columns: Optional[List[str]] = None, **kwargs
) -> pd.DataFrame:
    """
    Function to load data from csv and excel files
    """

    # Only set 'usecols' if target_columns is provided
    if target_columns is not None:
        kwargs["usecols"] = target_columns

    if filepath.suffix == ".csv":
        return pd.read_csv(filepath, **kwargs)

    elif filepath.suffix == ".xlsx":
        with suppress_openpyxl_warning():
            return pd.read_excel(filepath, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {filepath.suffix}")


def load_portfolio_lists(filepath: Path) -> Dict[str, list]:
    df = load_data(filepath)
    return {column: df[column].dropna().tolist() for column in df.columns}


def transform_dataframes(
    carteras: pd.DataFrame, crossreference: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    carteras = carteras.rename(
        columns={
            "Issuer Name": "issuer_name",
            "Filter Level 1": "filter_level_1",
            "Level": "level",
            "aladdin_id": "aladdin_id",
            "Security Description": "security_description",
            "Portfolio Full Name": "portfolio_full_name",
            "portfolio_id": "portfolio_id",
        }
    )
    carteras = carteras.loc[:, ["aladdin_id", "portfolio_id", "security_description"]]
    carteras = carteras.dropna(subset=["aladdin_id"])

    crossreference = crossreference.rename(
        columns={
            "Aladdin_Issuer": "aladdin_id",
            "Issuer_Name": "issuer_name",
            "CLARITY_AI": "permid",
            "MSCI": "msci",
            "SUST": "sust",
        }
    )
    crossreference = crossreference.loc[:, ["aladdin_id", "issuer_name", "permid"]]
    crossreference["permid"] = crossreference["permid"].astype(str)

    return carteras, crossreference


def check_data_types(df: pd.DataFrame, columns: list):
    for col in columns:
        if col in df.columns:
            logging.debug(f"Column {col} dtype: {df[col].dtype}")
            if df[col].dtype != "object":
                logging.warning(
                    f"Column {col} is not string type. Converting to string."
                )
                df[col] = df[col].astype(str)


def sanity_check(df: pd.DataFrame, strategy: str):
    excluded_count = df[df["strategy"] == strategy].shape[0]
    logging.debug(f"Sanity check for {strategy}: {excluded_count} excluded securities")
    return excluded_count


def filter_by_portfolio_lists(
    df: pd.DataFrame, portfolio_lists: Dict[str, list]
) -> pd.DataFrame:
    def process_portfolios(row):
        strategy = row["strategy"]
        affected_portfolios = set(row["affected portfolios"].split(", "))
        valid_portfolios = set(portfolio_lists.get(strategy, []))

        # Find the intersection of affected and valid portfolios
        valid_affected_portfolios = affected_portfolios.intersection(valid_portfolios)

        # If there's an intersection, update the row
        if valid_affected_portfolios:
            row["affected portfolios"] = ", ".join(sorted(valid_affected_portfolios))
            return row
        else:
            return pd.Series([None] * len(row), index=row.index)

    # Apply the process_portfolios function to each row
    processed_df = df.apply(process_portfolios, axis=1)

    # Remove rows where all values are None
    filtered_df = processed_df.dropna(how="all")

    return filtered_df


"""
============ DEFINE MAIN FUNCTION ============
"""


def main():
    # ADD BENCHMARKS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    start_time = time.time()
    # CONSTANTS
    # Get user input for date
    DATE = get_date()
    # Define input and output paths
    BASE_DIR = Path(r"C:\Users\n740789\Documents\Projects_local")
    REPO_DIR = Path(r"C:\Users\n740789\Documents\clarity_data_quality_controls")
    DATAFEED_DIR = BASE_DIR / "DataSets" / "DATAFEED" / "datafeeds_with_ovr"
    CROSSREF_DIR = BASE_DIR / "DataSets" / "crossreference"
    CARTERAS_DIR = BASE_DIR / "DataSets" / "aladdin_carteras_benchmarks"
    COMMITTEE_PATH = (
        REPO_DIR
        / "excel_books"
        / "sri_data"
        / "committee_portfolios"
        / "portfolio_lists.xlsx"
    )
    OUTPUT_DIR = BASE_DIR / "DataSets" / "incumplimientos"
    ALADDIN_DIR = (
        REPO_DIR
        / "excel_books"
        / "aladdin_data"
        / "{DATE}_strategies_snt world_portf_bmks.xlsx"
    )
    PORTFOLIO_LISTS_FILE = (
        BASE_DIR / "DataSets" / "committee_portfolios" / "portfolio_lists.xlsx"
    )

    with timer("Loading data files"):
        logging.info("Starting data processing")
        portfolio_lists = load_portfolio_lists(PORTFOLIO_LISTS_FILE)
        logging.info("committee portfolio list loaded")

        # Define target columns
        DATAFEED_TARGET_COLUMNS = [
            "permid",
            "clarityid",
            "issuer_name",
            "str_001_s",
            "str_002_ec",
            "str_003_ec",
            "str_004_asec",
            "str_005_ec",
            "str_006_sec",
            "str_007_sect",
            "cs_001_sec",
            "cs_002_ec",
            "cs_003_sec",
            "art_8_basicos",
            "gp_esccp_22",
            "str_003b_ec",
        ]

        # Load data
        # clarity datafeed
        datafeed = load_data(
            DATAFEED_DIR / f"{DATE}01_datafeed_with_ovr.csv",
            target_columns=DATAFEED_TARGET_COLUMNS,
            dtype="unicode",
        )
        datafeed.columns = datafeed.columns.str.lower()

        # BRS crossreference
        crossreference = load_data(
            CROSSREF_DIR / f"Aladdin_Clarity_Issuers_{DATE}01.csv",
            dtype={"CLARITY_AI": str},
        )

        # Aladdin data (portfolio) without strategies
        carteras = load_data(
            CARTERAS_DIR / f"{DATE}01_snt world_portf_bmks.xlsx",
            sheet_name="portfolio_carteras",
            skiprows=3,
        )

        # Aladdin data (portfolio and benchmarks) with strategies
        carteras_strategies = load_data(
            ALADDIN_DIR, sheet_name="portfolio_carteras", skiprows=3
        )
        benchmark_strategies = load_data(
            ALADDIN_DIR, sheet_name="portfolio_benchmarks", skiprows=3
        )

    with timer("Transforming dataframes"):
        carteras, crossreference = transform_dataframes(carteras, crossreference)

    strategies = {
        "STR001": "str_001_s",
        "STR002": "str_002_ec",
        "STR003": "str_003_ec",
        "STR004": "str_004_asec",
        "STR005": "str_005_ec",
        "STR006": "str_006_sec",
        "STR007": "str_007_sect",
        "CS001": "cs_001_sec",
        "ART8": "art_8_basicos",
        "STR003B": "str_003b_ec",
    }
    # we create this dictionary to match the format of the file were we save the record of noncompliances
    strategies_output_dict = {
        "STR001": "str_001_s",
        "STR002": "str_002_ec",
        "STR003": "str_003_ec",
        "STR004": "str_004_asec",
        "STR005": "str_005_ec",
        "STR006": "str_006_sec",
        "STR007": "str_007_sect",
        "CS001": "cs_001_sec",
        "ART8": "art_8_basicos",
        "STR003B": "str_003b_ec",
    }

    results = {}
    for strategy, column in strategies.items():
        logging.info(f"Processing strategy: {strategy}")
        excluded = datafeed_filtered[datafeed_filtered[column] == "EXCLUDED"]
        logging.debug(
            f"Number of excluded securities for {strategy}: {excluded.shape[0]}"
        )

        merged = pd.merge(
            excluded,
            crossreference,
            left_on="permid",
            right_on="permid",
            how="left",
            suffixes=("", "_crossreference"),
        )
        logging.debug(f"After merging with crossreference: {merged.shape[0]} rows")

        result = pd.merge(
            merged,
            carteras,
            left_on="aladdin_id",
            right_on="aladdin_id",
            how="inner",
            suffixes=("", "_car"),
        )
        logging.debug(f"After merging with car: {result.shape[0]} rows")

        result = result.rename(
            columns={
                "issuer_name": "issuer_name",
                "issuer_name_crossreference": "issuer_name_crossreference",
                "portfolio_id": "portfolio_id",
            }
        )

        # Check for duplicates
        duplicates = result.duplicated().sum()
        if duplicates > 0:
            logging.warning(
                f"Found {duplicates} duplicate rows in {strategy}. Removing duplicates."
            )
            result = result.drop_duplicates()

        grouped = (
            result.groupby(["issuer_name", "permid", "aladdin_id"])["portfolio_id"]
            .agg(lambda x: ", ".join(set(x)))
            .reset_index()
        )
        grouped["strategy"] = strategy
        grouped["date"] = DATE

        # Sanity check
        sanity_check(grouped, strategy)

        results[strategy] = grouped

    with timer("Creating final output"):
        all_results = pd.concat(results.values(), ignore_index=True)

        # Final filtering step to ensure only 'EXCLUDED' securities are included
        all_results = all_results[all_results["strategy"].isin(strategies.keys())]

        grouped_results = (
            all_results.groupby(
                ["strategy", "issuer_name", "permid", "aladdin_id", "date"]
            )["portfolio_id"]
            .first()
            .reset_index()
        )
        grouped_results.columns = [
            "strategy",
            "company",
            "permid",
            "aladdin_id",
            "date",
            "affected portfolios",
        ]
        grouped_results["result"] = ""
        grouped_results["action"] = ""
        grouped_results["exclusion ground"] = ""
        column_order = [
            "date",
            "strategy",
            "company",
            "result",
            "action",
            "exclusion ground",
            "permid",
            "aladdin_id",
            "affected portfolios",
        ]
        grouped_results = grouped_results[column_order]
        grouped_results.loc[:, "company"] = grouped_results.loc[
            :, "company"
        ].str.upper()

        # Convert "date" format from "202407" to "01/07/2024"
        grouped_results.loc[:, "date"] = pd.to_datetime(
            grouped_results.loc[:, "date"], format="%Y%m"
        ).dt.strftime("%d/%m/%Y")

        logging.info(
            f"Total number of excluded securities across all strategies: {grouped_results.shape[0]}"
        )

        # Filter results based on portfolio lists
        filtered_results = filter_by_portfolio_lists(grouped_results, portfolio_lists)
        logging.info(
            f"Filtered results based on portfolio lists. Remaining rows: {filtered_results.shape[0]}"
        )

        # Check if 'strategy' column exists
        if "strategy" not in filtered_results.columns:
            logging.error(
                "'strategy' column is missing after filtering. Cannot proceed."
            )
            return

        # Change the names of the strategy column's values with the strategies_output_dict
        filtered_results["strategy"] = (
            filtered_results["strategy"].map(strategies_output_dict).str.upper()
        )

        # Ensure all expected columns are present
        for col in column_order:
            if col not in filtered_results.columns:
                filtered_results[col] = ""  # Add missing columns with empty strings

        # Reorder columns
        filtered_results = filtered_results[column_order]

        output_file = OUTPUT_DIR / f"incumplimientos_{DATE}.csv"
        filtered_results.to_csv(output_file, index=False)

    end_time = time.time()
    logging.info(f"Script completed in {end_time - start_time:.2f} seconds")
    logging.info(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()
