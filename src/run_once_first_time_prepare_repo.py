#!/usr/bin/env python3
"""
prepare_repo.py – bootstrap folders, .venv and dependencies
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from typing import Iterable, List

# ── 1. User / date info ─────────────────────────────────────────────────────
N_USER = "n740789"  # adjust to your corporate UID

today = datetime.now()
YEAR = int(today.strftime("%Y"))
DATE = today.strftime("%Y%m")  # e.g. '202506'

# ── 2. Base directories ─────────────────────────────────────────────────────
REPO_DIR = Path(__file__).resolve().parents[1]  # ../
DATASETS_DIR = Path(rf"C:\Users\{N_USER}\Documents\Projects_local\datasets")
DATAFEED_DIR = DATASETS_DIR / "datafeeds"
IMPACT_ANALYSIS_DIR = DATASETS_DIR / "impact_analysis"

EXCEL_BOOKS_DIR = REPO_DIR / "excel_books"
SRI_DATA_DIR = EXCEL_BOOKS_DIR / "sri_data"
ALADDIN_DATA_DIR = EXCEL_BOOKS_DIR / "aladdin_data"
NASDAQ_DATA_DIR = EXCEL_BOOKS_DIR / "nasdaq_data"
SUSTAINALYTICS_DATA_DIR = EXCEL_BOOKS_DIR / "sustainalytics_data"

paths = {
    "CURRENT_DF_WOUTOVR_SEC_PATH": DATAFEED_DIR
    / "raw_dataset"
    / str(YEAR)
    / f"{DATE}01_Production",
    "PRE_DF_WOVR_PATH": DATAFEED_DIR / "datafeeds_with_ovr",
    "NEW_DF_WOVR_PATH": DATAFEED_DIR / "datafeeds_with_ovr",
    "DF_WOVR_PATH_DIR": DATAFEED_DIR / "datafeeds_with_ovr",
    "CURRENT_DF_WOUTOVR_PATH": DATAFEED_DIR / "datafeeds_without_ovr" / str(YEAR),
    "PROCESSED_DFS_WOUTOVR_PATH": DATAFEED_DIR / "datafeeds_without_ovr" / str(YEAR),
    "CROSSREFERENCE_PATH": ALADDIN_DATA_DIR / "crossreference",
    "BMK_PORTF_STR_PATH": ALADDIN_DATA_DIR / "bmk_portf_str",
    "OVR_PATH": SRI_DATA_DIR / "overrides",
    "COMMITTEE_PATH": SRI_DATA_DIR / "portfolios_committees",
    "NASDAQ_DATA_PATH": NASDAQ_DATA_DIR,
    "SUSTAINALYTICS_DATA_PATH": SUSTAINALYTICS_DATA_DIR,
}

REQUIREMENTS_TXT_PATH = REPO_DIR / "requirements.txt"


# ── 3. Helpers ──────────────────────────────────────────────────────────────
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    print(f"✓ ensured {path}")


def ensure_venv() -> Path:
    venv_dir = REPO_DIR / ".venv"
    if not venv_dir.exists():
        print("Creating virtual environment …")
        subprocess.run(
            [sys.executable, "-m", "venv", str(venv_dir)],
            check=True,
        )
    return venv_dir


def pip_install(venv_dir: Path) -> None:
    python_bin = venv_dir / ("Scripts" if os.name == "nt" else "bin") / "python"
    cmd = [
        str(python_bin),
        "-m",
        "pip",
        "install",
        "--trusted-host",
        "nexus.alm.europe.cloudcenter.corp",
        "-i",
        "http://nexus.alm.europe.cloudcenter.corp/repository/pypi-public/simple",
        "-r",
        f"{REQUIREMENTS_TXT_PATH}",
    ]
    print("Installing requirements …")
    subprocess.run(cmd, cwd=REPO_DIR, check=True)
    print("✓ dependencies installed")


# ── 4. Dated-directory generator ────────────────────────────────────────────
def gen_dated_dir(
    parent_dir: Path,
    year: int,
    *levels: List[str],
    months: Iterable[int] | None = None,
) -> None:
    """
    Creates every combination of *levels* inside parent_dir/year/YYMM/…
    For January 2025 the inner folder becomes '2501', December '2512'.
    """
    if months is None:
        months = range(1, 13)
    for m in months:
        yyyymm = f"{year % 100:02d}{m:02d}"  # 2501 … 2512
        for combo in product(*levels):
            path = parent_dir / yyyymm
            for part in combo:
                path /= part
            path.mkdir(parents=True, exist_ok=True)


# ── 5. Main workflow ────────────────────────────────────────────────────────
def main() -> None:
    print(f"Base repository: {REPO_DIR}")
    # core folders
    ensure_dir(DATAFEED_DIR)
    for p in paths.values():
        ensure_dir(p)

    # impact_analysis/YYMM/aladdin_input/<child> …
    gen_dated_dir(
        IMPACT_ANALYSIS_DIR,
        YEAR,
        ["aladdin_input"],
        ["art8", "esg", "responsable", "sustainable_004", "sustainable_007"],
    )

    venv_dir = ensure_venv()
    pip_install(venv_dir)
    print("Setup completed successfully.")


if __name__ == "__main__":
    main()
