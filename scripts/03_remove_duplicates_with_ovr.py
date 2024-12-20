import argparse
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


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


def process_data(df):
    df.columns = df.columns.str.lower()
    return df.drop_duplicates(subset=["permid"])


def save_data(df, csv_path):
    logging.info("Saving dataset at issuer level on a csv file")
    df.to_csv(csv_path, index=False)


def main():
    setup_logging()
    start_time = time.time()
    logging.info("Script started")

    # CONSTANTS
    # Get user input for date
    DATE = get_date()
    # Define input and output paths
    BASE_DIRECTORY = Path(
        r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ovr"
    )
    INPUT_PATH = BASE_DIRECTORY / f"{DATE}01_datafeed_with_ovr.csv"
    OUTPUT_PATH = BASE_DIRECTORY / f"{DATE}_df_issuer_level_with_ovr.csv"

    logging.info("Loading raw dataset")
    df = pd.read_csv(INPUT_PATH, low_memory=False)

    logging.info("Removing duplicates by permId")
    df_2 = process_data(df)

    save_data(df_2, OUTPUT_PATH)

    end_time = time.time()
    logging.info(f"Script completed in {end_time - start_time:.2f} seconds")
    logging.info(f"\nRows before: {df.shape[0]} \nRows after: {df_2.shape[0]}")
    logging.info(f"Files saved in {BASE_DIRECTORY}")


if __name__ == "__main__":
    main()
