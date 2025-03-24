import logging
from pathlib import Path
import pandas as pd

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


def load_clarity_data(file_path: Path) -> pd.DataFrame:
    """
    Load Clarity data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing Clarity data.

    Returns:
        pd.DataFrame: DataFrame with the Clarity data.
    """
    logger.info("Loading Clarity data from: %s", file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception:
        logger.exception("Failed to load Clarity data from: %s", file_path)
        raise
    logger.info("Successfully loaded Clarity data from: %s", file_path)
    return df


def load_aladdin_data(file_path: Path) -> pd.DataFrame:
    """
    Load Aladdin data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing Aladdin data.

    Returns:
        pd.DataFrame: DataFrame with the Aladdin data.
    """
    logger.info("Loading Aladdin data from: %s", file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception:
        logger.exception("Failed to load Aladdin data from: %s", file_path)
        raise
    logger.info("Successfully loaded Aladdin data from: %s", file_path)
    return df


def load_crossreference(file_path: Path) -> pd.DataFrame:
    """
    Load Cross Reference data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing cross reference data.

    Returns:
        pd.DataFrame: DataFrame with the cross reference data.
    """
    logger.info("Loading Cross Reference data from: %s", file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception:
        logger.exception("Failed to load Cross Reference data from: %s", file_path)
        raise
    logger.info("Successfully loaded Cross Reference data from: %s", file_path)
    return df


def load_portfolios(file_path: Path) -> pd.DataFrame:
    """
    Load Portfolio data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing portfolio data.

    Returns:
        pd.DataFrame: DataFrame with the portfolio data.
    """
    logger.info("Loading Portfolios data from: %s", file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception:
        logger.exception("Failed to load Portfolios data from: %s", file_path)
        raise
    logger.info("Successfully loaded Portfolios data from: %s", file_path)
    return df


def load_overrides(file_path: Path) -> pd.DataFrame:
    """
    Load Overrides data from a CSV file into a DataFrame.

    Parameters:
        file_path (Path): Path to the CSV file containing overrides data.

    Returns:
        pd.DataFrame: DataFrame with the overrides data.
    """
    logger.info("Loading Overrides data from: %s", file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception:
        logger.exception("Failed to load Overrides data from: %s", file_path)
        raise
    logger.info("Successfully loaded Overrides data from: %s", file_path)
    return df
