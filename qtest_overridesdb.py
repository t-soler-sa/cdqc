import warnings
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys


# set up logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


# define functio to get date from user
def validate_year_month(year_month: str) -> bool:
    """Validate the input year_month format."""
    try:
        datetime.strptime(year_month, "%Y%m")
        return True
    except ValueError:
        return False


# define function to get date from user
def get_year_month() -> str:
    while True:
        user_input = input("Please insert date with the format yyyymm: ")
        if validate_year_month(user_input):
            return user_input
        logging.warning("Invalid date format entered. Please try again.")


def main():
    # define path
    base_dir = Path(r"C:\Users\n740789\Documents\Projects_local\DataSets\overrides")

    # get date from user
    if len(sys.argv) > 1:
        year_month = sys.argv[1]
    else:
        year_month = get_year_month()

    input_file = base_dir / f"{year_month}_BBDD_Overrides.xlsx"

    if not input_file.exists():
        logging.error(f"Error: Input file {input_file} does not exist.")
        return

    # define columns
    columns = ["#", "ClarityID", "permId (Clarity)", "IssuerID", "Issuer Name"]

    # read the input file
    df = pd.read_excel(input_file, sheet_name="MAPEO P-S", usecols=columns)

    # check number of nan values in col ClarityID & permId (Clarity)
    nan_values = df[["ClarityID", "permId (Clarity)"]].isnull().sum()

    # check number col ClarityID == "-"
    clarityid_unknown = df[df["ClarityID"] == "-"].shape[0]
    # check number col permId (Clarity) == "-"
    permid_unknown = df[df["permId (Clarity)"] == "-"].shape[0]

    # print the number of nan values
    logging.info(f"Number of empty values in ClarityID: {nan_values['ClarityID']}")
    logging.info(f"Number of empty values in permId: {nan_values['permId (Clarity)']}")

    # print the number of unknown values
    logging.info(f"Number of unknown values in ClarityID: {clarityid_unknown}")
    logging.info(f"Number of unknown values in permId: {permid_unknown}")

    # finish script
    logging.info("Script finished successfully.")


if __name__ == "__main__":
    setup_logging()
    main()
