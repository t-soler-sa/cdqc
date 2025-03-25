import logging
import re
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

# Module-level logger
logger = logging.getLogger(__name__)


# define aux functions to clean columns and convert id columns into string
def clean_columns(columns):
    """
    Standardizes a list of column names:
    - Strips leading/trailing whitespace.
    - Converts to lowercase.
    - Replaces any sequence of whitespace (including newlines) with a single underscore.
    """
    return [re.sub(r"\s+", "_", col.strip().lower()) for col in columns]


def convert_id_columns(df):
    """
    Converts columns that end with '_id' or 'id' to string dtype,
    if they are not already strings.
    """
    pattern = re.compile(r"(_)?id$", re.IGNORECASE)
    for column in df.columns:
        if pattern.search(column) and not pd.api.types.is_string_dtype(df[column]):
            logger.info(f"Converting column '{column}' to string.")
            df[column] = df[column].astype(str)
    return df


def clean_and_convert(df):
    """
    First standardizes the DataFrame's column names, then converts any
    id columns to string dtype.
    """
    df.columns = clean_columns(df.columns)
    df = convert_id_columns(df)
    return df


# define functions to load data from different sources
def load_excel(
    file_path: Path, sheet_name: str, clean_n_convert: bool = True
) -> pd.DataFrame:
    """
    Read the specified sheet from an Excel file into a DataFrame.

    Parameters:
        file_path (Path): Path to the Excel file.
        sheet_name (str): Name of the sheet to read.

    Returns:
        pd.DataFrame: DataFrame containing the data from the sheet.
    """
    logger.info("Attempting to read Excel file: %s, sheet: %s", file_path, sheet_name)
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        logger.info(
            "Successfully read Excel file: %s, sheet: %s", file_path, sheet_name
        )
    except Exception:
        logger.exception(
            "Failed to read Excel file: %s, sheet: %s", file_path, sheet_name
        )
        raise
    if clean_n_convert:
        try:
            logger.info("Succesfully loaded a clean and converted the DataFrame.")
            return clean_and_convert(df)
        except Exception:
            logger.exception("Failed to clean and convert the DataFrame.")
            raise
    else:
        logger.info("Succesfully loaded the DataFrame.")
        return df


def load_clarity_data(file_path: Path, target_cols: list[str] = None) -> pd.DataFrame:
    """
    Load Clarity data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing Clarity data.
        target_cols (list[str], optional): List of columns to read from the CSV file.
            If None, read all columns. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing the Clarity data.
    """
    logger.info("Loading Clarity data from: %s", file_path)
    try:
        if target_cols:
            df = pd.read_csv(
                file_path, usecols=target_cols, dtype={"permid": str, "isin": str}
            )
        else:
            df = pd.read_csv(
                file_path,
                dtype={
                    "permid": str,
                    "permId": str,
                    "isin": str,
                    "ISIN": str,
                    "ClarityID": str,
                    "clarityid": str,
                },
            )
            df.columns = clean_columns(df.columns)
    except Exception:
        logger.exception("Failed to load Clarity data from: %s", file_path)
        raise
    logger.info("Successfully loaded Clarity data from: %s", file_path)
    return df


def load_aladdin_data(file_path: Path, sheet_name: str) -> pd.DataFrame:
    """
    Load Aladdin data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing Aladdin data.
        sheet_name (str): Name of the sheet to read.

    Returns:
        pd.DataFrame: DataFrame with the Aladdin data.
    """
    logger.info(f"Loading {sheet_name} data from {file_path}")
    try:
        df = pd.read_excel(
            file_path, dtype="unicode", sheet_name=sheet_name, skiprows=3
        )
        logger.info(f"Cleaning columns and converting data types for {sheet_name}")
        df = clean_and_convert(df)
    except Exception:
        logger.exception(f"Failed to load {sheet_name} data from {file_path}")
        raise

    logger.info("Successfully loaded Aladdin data from: %s", file_path)
    return df


def load_crossreference(file_path: Path) -> pd.DataFrame:
    """
    Load crossreference data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing crossreference data.

    Returns:
        pd.DataFrame: DataFrame with the crossreference data and columns renamed
    """
    logger.info("Loading crossreference data from: %s", file_path)
    try:
        df = pd.read_csv(file_path, dtype=str)
    except Exception:
        logger.exception("Failed to load crossreference data from: %s", file_path)
        raise
    logger.info("Cleaning columns and renaming crossreference data")
    df.columns = clean_columns(df.columns)
    df.rename(
        columns={"clarity_ai": "permid", "aladdin_issuer": "aladdin_id"}, inplace=True
    )
    logger.info("Successfully loaded crossreference from: %s", file_path)
    return df


def load_portfolios(
    path: Path,
) -> Tuple[Dict[str, List[Any]], Dict[str, List[Any]], List[Any], List[Any], List[Any]]:
    """
    Loads portfolio and benchmark data from an Excel file located at 'path'
    and returns the following:
      - portfolios_dict: dictionary of portfolios (lists with 'nan' strings removed)
      - benchmarks_dict: dictionary of benchmarks (lists with 'nan' strings removed)
      - carteras_list: flat list of all portfolio items
      - benchmarks_list: flat list of all benchmark items
      - carteras_benchmarks_list: concatenation of carteras_list and benchmarks_list

    Parameters:
        path (str): The file path to the Excel workbook.

    Returns:
        tuple: A tuple containing:
            - portfolios_dict (dict)
            - benchmarks_dict (dict)
            - carteras_list (list)
            - benchmarks_list (list)
            - carteras_benchmarks_list (list)
    """
    try:
        # Read the Excel sheets using the provided path
        logger.info("Loading portfolios portfolio_carteras from: %s", path)
        portfolios = pd.read_excel(path, sheet_name="portfolio_carteras", dtype=str)
    except Exception:
        logger.exception("Failed to load portfolios from: %s", path)
        raise
    try:
        logger.info("Loading benchmarks from: %s", path)
        benchmarks = pd.read_excel(path, sheet_name="portfolio_benchmarks", dtype=str)
    except Exception:
        logger.exception("Failed to load benchmarks from: %s", path)
        raise

    # Convert DataFrames to dicts with list values
    logger.info("Converting portfolios and benchmarks to dictionaries")
    portfolios_dict = portfolios.to_dict(orient="list")
    benchmarks_dict = benchmarks.to_dict(orient="list")

    # Remove 'nan' strings from the lists in the dictionaries
    logger.info("Removing 'nan' strings from the lists")
    portfolios_dict = {
        k: [x for x in v if str(x) != "nan"] for k, v in portfolios_dict.items()
    }
    benchmarks_dict = {
        k: [x for x in v if str(x) != "nan"] for k, v in benchmarks_dict.items()
    }

    # Create flat lists for portfolios and benchmarks, and a combined list
    logger.info("Creating flat lists for portfolios and benchmarks")
    carteras_list = list(chain(*portfolios_dict.values()))
    benchmarks_list = list(chain(*benchmarks_dict.values()))
    carteras_benchmarks_list = carteras_list + benchmarks_list

    return (
        portfolios_dict,
        benchmarks_dict,
        carteras_list,
        benchmarks_list,
        carteras_benchmarks_list,
    )


def load_overrides(file_path: Path) -> pd.DataFrame:
    """Load overrides from a CSV file."""
    target_cols = ["clarityid", "permid", "brs_id", "ovr_target", "ovr_value"]
    print(f"loading overrides columns {target_cols}")
    try:
        logger.info(f"Loading overrides from: {file_path}")
        df = pd.read_excel(
            file_path,
            usecols=target_cols,
            dtype={
                "clarityid": str,
                "permid": str,
                "brs_id": str,
                "ovr_target": str,
                "ovr_value": str,
            },
        )
    except Exception:
        logger.exception(f"Failed to load overrides from: {file_path}")
        raise
    return df
