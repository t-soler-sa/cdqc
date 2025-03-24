import logging
from pathlib import Path
import pandas as pd
from typing import List, Tuple, Dict, Any
from itertools import chain

# Module-level logger
logger = logging.getLogger(__name__)


def read_excel(file_path: Path, sheet_name: str) -> pd.DataFrame:
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
    except Exception:
        logger.exception(
            "Failed to read Excel file: %s, sheet: %s", file_path, sheet_name
        )
        raise
    logger.info("Successfully read Excel file: %s, sheet: %s", file_path, sheet_name)
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
            df = pd.read_csv(file_path, usecols=target_cols)
        else:
            df = pd.read_csv(file_path)
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
    except Exception:
        logger.exception(f"Failed to load {sheet_name} data from {file_path}")
        raise
    logger.info(f"editting column names for {sheet_name} data")
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    logger.info("Successfully loaded Aladdin data from: %s", file_path)
    return df


def load_crossreference(file_path: Path) -> pd.DataFrame:
    """
    Load Cross Reference data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing cross reference data.

    Returns:
        pd.DataFrame: DataFrame with the cross reference data and columns renamed
    """
    logger.info("Loading Cross Reference data from: %s", file_path)
    try:
        df = pd.read_csv(file_path, dtype={"CLARITY_AI": str})
    except Exception:
        logger.exception("Failed to load Cross Reference data from: %s", file_path)
        raise
    logger.info("editting crossreference's column names")
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
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
