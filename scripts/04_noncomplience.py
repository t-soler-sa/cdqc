import argparse
import logging
import os
import time
import warnings
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

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


def validate_date(date_string):
    try:
        datetime.strptime(date_string, "%Y%m")
        return True
    except ValueError:
        return False


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


def load_dataframe(filepath: Path, **kwargs) -> pd.DataFrame:
    if filepath.suffix == ".csv":
        return pd.read_csv(filepath, **kwargs)
    elif filepath.suffix == ".xlsx":
        return pd.read_excel(filepath, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {filepath.suffix}")


def load_portfolio_lists(filepath: Path) -> Dict[str, list]:
    df = load_dataframe(filepath)
    return {column: df[column].dropna().tolist() for column in df.columns}


def transform_dataframes(
    car: pd.DataFrame, xref: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    car = car.rename(
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
    car = car.loc[:, ["aladdin_id", "portfolio_id", "security_description"]]
    car = car.dropna(subset=["aladdin_id"])

    xref = xref.rename(
        columns={
            "Aladdin_Issuer": "aladdin_id",
            "Issuer_Name": "issuer_name",
            "CLARITY_AI": "permid",
            "MSCI": "msci",
            "SUST": "sust",
        }
    )
    xref = xref.loc[:, ["aladdin_id", "issuer_name", "permid"]]
    xref["permid"] = xref["permid"].astype(str)

    return car, xref


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


def main():
    start_time = time.time()
    # CONSTANTS
    # Get user input for date
    DATE = get_date()
    # Define input and output paths
    BASE_DIR = Path(r"C:\Users\n740789\Documents\Projects_local")
    DATAFEED_DIR = BASE_DIR / "DataSets" / "DATAFEED" / "datafeeds_with_ovr"
    CROSSREF_DIR = BASE_DIR / "DataSets" / "crossreference"
    CARTERAS_DIR = BASE_DIR / "DataSets" / "aladdin_carteras_benchmarks"
    OUTPUT_DIR = BASE_DIR / "DataSets" / "incumplimientos"
    PORTFOLIO_LISTS_FILE = (
        BASE_DIR / "DataSets" / "committee_portfolios" / "portfolio_lists.xlsx"
    )

    with timer("Loading data files"):
        logging.info("Starting data processing")
        portfolio_lists = load_portfolio_lists(PORTFOLIO_LISTS_FILE)
        logging.info("committee portfolio list loaded")
        datafeed = load_dataframe(
            DATAFEED_DIR / f"{DATE}01_datafeed_with_ovr.csv", dtype="unicode"
        )
        datafeed.columns = datafeed.columns.str.lower()

        # Add this filtering step
        columns_to_keep = [
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
        datafeed_filtered = datafeed[columns_to_keep]
        logging.info(
            f"Filtered datafeed to {len(columns_to_keep)} columns. Shape: {datafeed_filtered.shape}"
        )

        xref = load_dataframe(
            CROSSREF_DIR / f"Aladdin_Clarity_Issuers_{DATE}01.csv",
            dtype={"CLARITY_AI": str},
        )
        with suppress_openpyxl_warning():
            car = load_dataframe(
                CARTERAS_DIR / f"{DATE}01_snt world_portf_bmks.xlsx",
                sheet_name="portfolio_carteras",
                skiprows=3,
            )

    with timer("Transforming dataframes"):
        car, xref = transform_dataframes(car, xref)

    strategies = {
        "STR001": "str_001_s",
        "STR002": "str_002_ec",
        "STR003": "str_003_ec",
        "STR004": "str_004_asec",
        "STR004_SB": "str_004_asec",
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
        "STR004_SB": "STR_004_GREEN",
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
            xref,
            left_on="permid",
            right_on="permid",
            how="left",
            suffixes=("", "_xref"),
        )
        logging.debug(f"After merging with xref: {merged.shape[0]} rows")

        result = pd.merge(
            merged,
            car,
            left_on="aladdin_id",
            right_on="aladdin_id",
            how="inner",
            suffixes=("", "_car"),
        )
        logging.debug(f"After merging with car: {result.shape[0]} rows")

        result = result.rename(
            columns={
                "issuer_name": "issuer_name",
                "issuer_name_xref": "issuer_name_xref",
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
