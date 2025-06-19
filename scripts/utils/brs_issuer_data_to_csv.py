"""
Script to consolidate BRS issuer data from Excel to CSV.

Changes (2025‑06‑04)
-------------------
* **New output layout** – issuer‑level primary key with list columns:
  * ``issuer_id`` (PK)
  * ``ultimate_issuer_id``
  * ``issuer_name``
  * ``ptf_ids_list`` – list of portfolio IDs
  * ``bmk_ids_list`` – list of benchmark IDs
* Removes the previous ``ptf_bmk_id`` / ``ptf_bmk_flag`` columns by
  aggregating them into the two list columns.

Steps
-----
1. Load data from the Excel workbook (sheets ``bmk`` and ``ptf``).
2. Clean & normalise each sheet (same rules as before).
3. Concatenate the sheets.
4. Aggregate to issuer‑level lists for portfolio / benchmark IDs.
5. Write the dated CSV.​

Usage
-----
Run as a module from your project root::

    python -m scripts.utils.brs_issuer_data_to_csv

You can also import and call :pyfunc:`main` from elsewhere in your code.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import List, Sequence

import warnings

# import openpyxl

import pandas as pd

from .config import get_config

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

CONFIG = get_config(
    "brs_issuer_data_to_csv",
)

logger = CONFIG["logger"]
DATE_STAMP_YM = CONFIG["DATE"]
# --------------------------------------------------------------------------- #
BASE_DIR: Path = CONFIG["BRS_ISSUER_DATA_DIR_PATH"]
IN_FILE: Path = BASE_DIR / f"{DATE_STAMP_YM}_snt_world_sntcor_corp_shares.xlsx"
OUT_FILE: Path = BASE_DIR / f"{DATE_STAMP_YM}_brs_issuer_data.csv"

# Columns expected after the cleaning step (before aggregation)
_INTERMEDIATE_COLS = [
    "issuer_id",
    "ultimate_issuer_id",
    "issuer_name",
    "ptf_bmk_id",
    "ptf_bmk_flag",
]

# Final output schema
TARGET_COLS_OUT = [
    "issuer_id",
    "ultimate_issuer_id",
    "issuer_name",
    "ptf_ids_list",
    "bmk_ids_list",
]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned issuer DataFrame in the *intermediate* schema.

    The cleaning rules are unchanged from the previous version; see the
    docstring at the top of this file for details.
    """
    # Harmonise column names
    df.columns = df.columns.str.strip().str.lower()

    # Guard against missing mandatory columns early
    missing = {"issuer_id", "ultimate_issuer_id", "issuer_name"}.difference(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {', '.join(sorted(missing))}")

    # Drop rows without issuer_id or with issuer_id == "SNT-WORLD"
    df = df.dropna(subset=["issuer_id"])
    df = df[~df["issuer_id"].str.upper().eq("SNT-WORLD")]

    # Keep only rows belonging to the SNT Core corporate universe
    if "sntcore_share_corps_flag" in df.columns:
        df = df[df["sntcore_share_corps_flag"].str.upper().eq("TRUE")]
    else:
        logger.warning("'sntcore_share_corps_flag' not found; retaining all rows.")

    # Identify portfolio / benchmark and create ptf_bmk_id + ptf_bmk_flag
    if "portfolio_id" in df.columns:
        df = df.rename(columns={"portfolio_id": "ptf_bmk_id"})
        df["ptf_bmk_flag"] = "portfolio"
    elif "benchmark_id" in df.columns:
        df = df.rename(columns={"benchmark_id": "ptf_bmk_id"})
        df["ptf_bmk_flag"] = "benchmark"
    else:
        logger.warning(
            "Neither 'portfolio_id' nor 'benchmark_id' present; setting blanks."
        )
        df["ptf_bmk_id"] = pd.NA
        df["ptf_bmk_flag"] = pd.NA

    # Ensure the schema is complete and ordered
    for col in _INTERMEDIATE_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[_INTERMEDIATE_COLS]


def _list_or_empty(values: Sequence[str]) -> list[str]:
    """Return a *sorted unique* list – or an empty list if none are valid."""
    clean = sorted({v for v in values if pd.notna(v) and v != ""})
    return clean


# --------------------------------------------------------------------------- #
# Main workflow
# --------------------------------------------------------------------------- #


def main() -> None:
    logger.info(
        "Load, clean, merge, aggregate, and export BRS issuer data for date %s.",
        DATE_STAMP_YM,
    )
    logger.info("Loading BRS issuer data from %s", IN_FILE)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            issuers_bmk = pd.read_excel(
                IN_FILE, sheet_name="bmk", skiprows=3, dtype=str
            )
            issuers_ptf = pd.read_excel(
                IN_FILE, sheet_name="ptf", skiprows=3, dtype=str
            )
    except FileNotFoundError:
        logger.exception("Excel source file not found: %s", IN_FILE)
        sys.exit(1)

    logger.info("Cleaning individual sheets …")
    cleaned_frames: List[pd.DataFrame] = [
        _clean_df(df) for df in (issuers_bmk, issuers_ptf)
    ]

    logger.info("Concatenating cleaned sheets …")
    intermediate = pd.concat(cleaned_frames, ignore_index=True, copy=False)

    # --------------------------------------------------------------------- #
    # Aggregate to issuer‑level lists
    # --------------------------------------------------------------------- #
    logger.info("Aggregating portfolio / benchmark IDs into list columns …")

    def _agg_lists(group: pd.DataFrame) -> pd.Series:
        ptf_ids = _list_or_empty(
            group.loc[group["ptf_bmk_flag"].eq("portfolio"), "ptf_bmk_id"]
        )
        bmk_ids = _list_or_empty(
            group.loc[group["ptf_bmk_flag"].eq("benchmark"), "ptf_bmk_id"]
        )
        return pd.Series(
            {
                "ptf_ids_list": ptf_ids,
                "bmk_ids_list": bmk_ids,
            }
        )

    consolidated = (
        intermediate.groupby(
            ["issuer_id", "ultimate_issuer_id", "issuer_name"],
            sort=False,
            as_index=False,
        )
        .apply(_agg_lists)
        .reset_index(drop=True)
    )

    # Optionally convert list columns to stringified lists for CSV readability
    consolidated["ptf_ids_list"] = consolidated["ptf_ids_list"].apply(
        lambda lst: str(lst)
    )
    consolidated["bmk_ids_list"] = consolidated["bmk_ids_list"].apply(
        lambda lst: str(lst)
    )

    # Ensure output directory exists
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Writing consolidated CSV to %s", OUT_FILE)
    consolidated.to_csv(OUT_FILE, index=False, columns=TARGET_COLS_OUT)
    logger.info("Done – saved %d issuer rows", len(consolidated))


if __name__ == "__main__":
    main()
