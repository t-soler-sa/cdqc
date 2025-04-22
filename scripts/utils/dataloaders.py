import logging
import re
import warnings
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from datetime import datetime

import pandas as pd

# Module-level logger
logger = logging.getLogger(__name__)

# Ignore workbook warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


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


def load_excel(
    file_path: Path, sheet_name: str, clean_n_convert: bool = True
) -> pd.DataFrame:
    """
    Read the specified sheet from an Excel file into a DataFrame.

    Parameters:
        file_path (Path): Path to the Excel file.
        sheet_name (str): Name of the sheet to read.
        clean_n_convert (bool): If True, clean and convert the DataFrame.

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


def load_csv(
    file_path: Path, clean_n_convert: bool = True, low_memory: bool = False
) -> pd.DataFrame:
    """
    Read the specified sheet from an csv file into a DataFrame.

    Parameters:
        file_path (Path): Path to the csv file.
        clean_n_convert (bool): If True, clean and convert the DataFrame.
        low_memory (bool): If True, use low memory mode when reading the csv file.

    Returns:
        pd.DataFrame: DataFrame containing the data from the sheet.
    """
    logger.info("Attempting to read csv file: %s", file_path)
    try:
        df = pd.read_csv(file_path, low_memory=low_memory)
        logger.info("Successfully read csv file: %s", file_path)
    except Exception:
        logger.exception("Failed to read csv file: %s", file_path)
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
                low_memory=False,
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


def load_overrides(file_path: Path, target_cols: list[str] = None) -> pd.DataFrame:
    """Load overrides from a CSV file."""
    if target_cols is None:
        # Default columns to load if not specified
        target_cols = [
            "clarityid",
            "permid",
            "brs_id",
            "ovr_target",
            "ovr_value",
            "ovr_active",
        ]
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
                "ovr_active": bool,
            },
        )
    except Exception:
        logger.exception(f"Failed to load overrides from: {file_path}")
        raise

    # return only active overrides
    df = df[df["ovr_active"] == True].copy()
    # remove column "ovr_active"
    df.drop(columns=["ovr_active"], inplace=True)

    return df


def load_portfolios(
    path_pb: Path,
    path_committe: Path,
    target_cols_portfolio: List[str] = None,
    target_cols_benchmarks: List[str] = None,
) -> Tuple[
    Dict[str, Dict[str, Union[List[str], str]]],
    Dict[str, Dict[str, Union[List[str], str]]],
]:
    """
    Loads portfolio and benchmark data from an Excel file located at 'path_pb',
    and loads strategy information from 'path_committe'. Returns two dictionaries:
    1) portfolio_dict
    2) benchmark_dict

    Each dictionary maps:
        - portfolio_id (or benchmark_id) -> {
              "aladdin_id": list of associated aladdin_ids,
              "strategy_name": the single strategy name from the 'portfolio_lists.xlsx'
                               or an empty string if none is found
          }

    Parameters:
        path_pb (Path):
            The file path to the Excel workbook containing the portfolio_carteras
            and portfolio_benchmarks sheets.
        path_committe (Path):
            The file path to the Excel workbook containing the 'Portfolios' and
            'Benchmarks' sheets that map each ID to a strategy.
        target_cols_portfolio (list, optional):
            Columns to read from the portfolio_carteras sheet.
            Defaults to ["aladdin_id", "portfolio_id"].
        target_cols_benchmarks (list, optional):
            Columns to read from the portfolio_benchmarks sheet.
            Defaults to ["aladdin_id", "benchmark_id"].

    Returns:
        (portfolio_dict, benchmark_dict): Each a dict of the form:
            {
              "<portfolio_id or benchmark_id>": {
                "aladdin_id": [...],
                "strategy_name": "STRXXX" or ""
              },
              ...
            }
    """

    # 0 define aux function filter strategy entries
    def filter_strategy_entries(data: dict, key_name: str = "strategy_name") -> dict:
        logger.info("Filtering strategy entries")
        """
        Filters a dictionary of entries to only include those entries 
        where the value corresponding to key_name is non-empty.

        Parameters:
            data (dict): A dictionary where each key maps to another dictionary 
                         containing at least the keys 'aladdin_id' and key_name.
            key_name (str): The key to check for a non-empty value. Defaults to 'strategy_name'.

        Returns:
            dict: A new dictionary with entries that have non-empty values for key_name.
        """
        filtered_data = {}
        for entry_key, entry_value in data.items():
            # Check if the entry has the key_name and that its value is non-empty.
            if key_name in entry_value and entry_value[key_name]:
                filtered_data[entry_key] = entry_value

        logger.info("Dictonary cleaned of empty strategies")
        return filtered_data

    # 1. Set default columns if none are provided
    if target_cols_portfolio is None:
        target_cols_portfolio = ["aladdin_id", "portfolio_id"]
    if target_cols_benchmarks is None:
        target_cols_benchmarks = ["aladdin_id", "benchmark_id"]

    # 2. Load the original portfolios and benchmarks from path_pb
    try:
        logger.info("Loading portfolios from: %s", path_pb)
        portfolios = pd.read_excel(
            path_pb,
            sheet_name="portfolio_carteras",
            usecols=target_cols_portfolio,
            dtype=str,
            skiprows=3,
        )
        portfolios.columns = clean_columns(portfolios.columns)
    except Exception:
        logger.exception("Failed to load portfolios from: %s", path_pb)
        raise

    try:
        logger.info("Loading benchmarks from: %s", path_pb)
        benchmarks = pd.read_excel(
            path_pb,
            sheet_name="portfolio_benchmarks",
            usecols=target_cols_benchmarks,
            dtype=str,
            skiprows=3,
        )
        benchmarks.columns = clean_columns(benchmarks.columns)
    except Exception:
        logger.exception("Failed to load benchmarks from: %s", path_pb)
        raise

    # Remove rows with missing or 'nan' values in aladdin_id or portfolio/benchmark ID
    portfolios = portfolios.dropna(subset=["aladdin_id", "portfolio_id"])
    benchmarks = benchmarks.dropna(subset=["aladdin_id", "benchmark_id"])

    # 3. Load the 'Portfolios' sheet from path_committe to build a map: portfolio_id -> strategy_name
    try:
        logger.info("Loading strategy data for portfolios from: %s", path_committe)
        portfolios_strategies = pd.read_excel(
            path_committe, sheet_name="Portfolios", dtype=str
        )
    except Exception:
        logger.exception("Failed to load 'Portfolios' sheet from: %s", path_committe)
        raise

    # 4. Build a dictionary: portfolio_id -> strategy_name from the 'Portfolios' sheet
    #    Each column is a strategy name, and the rows are portfolio IDs for that strategy.
    portfolio_strategy_map = {}
    for col in portfolios_strategies.columns:
        strategy_name = col.strip().lower()
        # get non-null portfolio IDs in this column
        ids_in_col = portfolios_strategies[col].dropna().unique()
        for pid in ids_in_col:
            pid_str = str(pid).strip()
            if pid_str not in portfolio_strategy_map:
                portfolio_strategy_map[pid_str] = strategy_name
            else:
                # If a portfolio appears in multiple columns, logg warning and continue
                logger.warning(
                    "Portfolio ID '%s' appears in multiple strategies", pid_str
                )
                pass

    # 5. Load the 'Benchmarks' sheet from path_committe to build a map: benchmark_id -> strategy_name
    try:
        logger.info("Loading strategy data for benchmarks from: %s", path_committe)
        benchmarks_strategies = pd.read_excel(
            path_committe, sheet_name="Benchmarks", dtype=str
        )
    except Exception:
        logger.exception("Failed to load 'Benchmarks' sheet from: %s", path_committe)
        raise

    # 6. Build a dictionary: benchmark_id -> list of strategy_names from the 'Benchmarks' sheet
    benchmark_strategy_map = {}
    for col in benchmarks_strategies.columns:
        strategy_name = col.strip().lower()
        # get non-null benchmark IDs in this column
        ids_in_col = benchmarks_strategies[col].dropna().unique()
        for bid in ids_in_col:
            bid_str = str(bid).strip()
            if bid_str not in benchmark_strategy_map:
                # If first time we see this benchmark, store its strategy in a new list
                benchmark_strategy_map[bid_str] = [strategy_name]
            else:
                # If it already exists, append the new strategy to the list
                logger.info("Benchmark ID '%s' appears in multiple strategies", bid_str)
                benchmark_strategy_map[bid_str].append(strategy_name)

    # 7. Group the original portfolios by portfolio_id so we can collect a list of aladdin_ids
    #    Example: for each portfolio_id, gather all aladdin_ids that map to it
    portfolio_dict = {}
    grouped_portfolios = portfolios.groupby("portfolio_id")["aladdin_id"].apply(list)

    for pid, aladdin_ids in grouped_portfolios.items():
        pid_str = str(pid).strip()
        # get the strategy name from the map; if not found, empty string
        strategy_name = portfolio_strategy_map.get(pid_str, "")
        portfolio_dict[pid_str] = {
            "aladdin_id": [aid for aid in aladdin_ids if pd.notna(aid)],
            "strategy_name": strategy_name,
        }

    # 8. Group the original benchmarks by benchmark_id
    benchmark_dict = {}
    grouped_benchmarks = benchmarks.groupby("benchmark_id")["aladdin_id"].apply(list)

    for bid, aladdin_ids in grouped_benchmarks.items():
        bid_str = str(bid).strip()
        strategy_name = benchmark_strategy_map.get(bid_str, "")
        benchmark_dict[bid_str] = {
            "aladdin_id": [aid for aid in aladdin_ids if pd.notna(aid)],
            "strategy_name": strategy_name,
        }

    # 9. Return the two dictionaries
    return filter_strategy_entries(portfolio_dict), filter_strategy_entries(
        benchmark_dict
    )


def save_excel(df_dict: dict, output_dir: Path, file_name: str) -> Path:
    """
    Writes multiple DataFrames to an Excel file with each DataFrame in a separate sheet.

    Parameters:
    - df_dict (dict): A dictionary where keys are sheet names and values are DataFrames.
    - output_dir (Path): The directory where the Excel file will be saved.
    - file_name (str): The base name for the Excel file.

    Returns:
    - Path: The full path to the saved Excel file.
    """
    # Create a date string in "YYYYMMDD" format
    date_str = datetime.now().strftime("%Y%m%d")

    # Ensure the output directory exists
    logger.info("Creating output directory: %s", output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Construct the full output file path (e.g., file_name_YYYYMMDD.xlsx)
    output_file = output_dir / f"{date_str}_{file_name}.xlsx"

    # Write each DataFrame to its own sheet with index set to False
    with pd.ExcelWriter(output_file) as writer:
        logger.info("Writing DataFrames to Excel file: %s", output_file)
        for sheet_name, df in df_dict.items():
            logger.info("Writing sheet: %s", sheet_name)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    logger.info("Results saved to Excel file: %s", output_file)
