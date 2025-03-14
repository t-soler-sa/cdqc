# dataloaders.py

import pandas as pd
from typing import List, Tuple, Dict, Any
from itertools import chain
from pathlib import Path


def read_excel(file_path: Path, sheet_name: str) -> pd.DataFrame:
    """Read the specified sheet from an Excel file into a DataFrame."""
    return pd.read_excel(file_path, sheet_name=sheet_name)


def load_clarity_data(file_path: str, columns: List[str]) -> pd.DataFrame:
    """Load data from CSV file."""
    return pd.read_csv(file_path, sep=",", dtype="unicode", usecols=columns)


def load_aladdin_data(file_path: str, sheet_name: str) -> pd.DataFrame:
    """Load data from an Excel file."""
    df = pd.read_excel(file_path, dtype="unicode", sheet_name=sheet_name, skiprows=3)
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    return df


def load_crossreference(file_path: str) -> pd.DataFrame:
    """Load cross reference data from CSV file."""
    df = pd.read_csv(file_path, dtype={"CLARITY_AI": str})
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    df.rename(
        columns={"clarity_ai": "permid", "aladdin_issuer": "aladdin_id"}, inplace=True
    )
    return df


def load_portfolios(
    path: str,
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
    # Read the Excel sheets using the provided path
    portfolios = pd.read_excel(path, sheet_name="Portfolios", dtype=str)
    benchmarks = pd.read_excel(path, sheet_name="Benchmarks", dtype=str)

    # Convert DataFrames to dicts with list values
    portfolios_dict = portfolios.to_dict(orient="list")
    benchmarks_dict = benchmarks.to_dict(orient="list")

    # Remove 'nan' strings from the lists in the dictionaries
    portfolios_dict = {
        k: [x for x in v if str(x) != "nan"] for k, v in portfolios_dict.items()
    }
    benchmarks_dict = {
        k: [x for x in v if str(x) != "nan"] for k, v in benchmarks_dict.items()
    }

    # Create flat lists for portfolios and benchmarks, and a combined list
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


def load_overrides(file_path: str) -> pd.DataFrame:
    """Load overrides from a CSV file."""
    return pd.read_csv(file_path)
