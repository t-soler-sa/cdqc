# monthly_missing_issuers.py

"""
Quick check of “lost” issuers month-to-month in a folder of ESG CSVs,
plus an Excel summary of the missing PERMIDs / issuer names.

Assumptions
-----------
* Filenames start with YYYYMM…csv   (e.g. 20250101_…csv → month 202501).
* Only columns needed are `permid` (string) and `issuer_name`.
* Logger helper `set_up_log` lives one level up, already configured.
"""

from pathlib import Path
import pandas as pd

from datetime import datetime


from .config import get_config

# -------------------------------------------------
script_name = Path(__file__).stem

config = get_config(
    "monthly_missing_issuers_clarity_datafeed_analysis",
    auto_date=False,
    fixed_date="202506",
)
logger = config["logger"]

DATA_DIR = Path(
    r"C:\Users\n740789\Documents\Projects_local\datasets\datafeeds\datafeeds_without_ovr\2025"
)

date = datetime.now().strftime("%Y%m%d")

EXCEL_OUT = Path(rf"C:\Users\n740789\Downloads\{date}_lost_issuers.xlsx")
# -------------------------------------------------


def month_tag(f: Path) -> str:
    """Return YYYYMM extracted from the file name."""
    return f.stem[:6]  # works for 20250101_*.csv


def load_df(f: Path) -> pd.DataFrame:
    """Read only the needed columns and index by permid."""
    df = pd.read_csv(
        f,
        usecols=["permid", "issuer_name"],
        dtype={"permid": "string"},  # choose plain string to avoid pyarrow dep
    )
    return df.set_index("permid", drop=False)


def main() -> None:
    files = sorted(DATA_DIR.glob("*.csv"), key=month_tag)

    # Collect the deltas so we can write all sheets in one go
    excel_sheets: dict[str, pd.DataFrame] = {}

    summary_logs = []

    for prev_file, next_file in zip(files, files[1:]):
        prev_month, next_month = month_tag(prev_file), month_tag(next_file)

        prev_df = load_df(prev_file)
        next_df = load_df(next_file)

        missing_ids = prev_df.index.difference(next_df.index)
        n_missing = len(missing_ids)

        logger_message = (
            f"From {prev_month} to {next_month}, " f"{n_missing} issuers were lost"
        )
        logger.info(logger_message)
        summary_logs.append(logger_message)

        if n_missing == 0:
            continue  # nothing to record this round

        # Build a small dataframe with the lost rows
        lost_df = prev_df.loc[missing_ids, ["permid", "issuer_name"]].reset_index(
            drop=True
        )

        # Sheet name: lost_0102, lost_0203, etc. (4 digits = oldMM newMM)
        sheet_suffix = f"{prev_month[-2:]}{next_month[-2:]}"
        sheet_name = f"lost_{sheet_suffix}"

        # Keep for Excel writing later
        excel_sheets[sheet_name] = lost_df

        # Also log every lost issuer
        for _, row in lost_df.iterrows():
            logger.info(f"{row.permid} | {row.issuer_name}")

    # -------- write the Excel file --------
    if excel_sheets:
        with pd.ExcelWriter(EXCEL_OUT, engine="xlsxwriter") as writer:
            for sheet, df in excel_sheets.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        logger.info(f"Excel summary written to {EXCEL_OUT}\n")
    else:
        logger.info("No missing issuers detected; Excel file was not created.")

    logger.info("\n\nSummary of missing issuers:\n\n")
    for msg in summary_logs:
        logger.info(msg)


if __name__ == "__main__":
    main()
