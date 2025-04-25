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

from .set_up_log import set_up_log

# -------------------------------------------------
script_name = Path(__file__).stem
logger = set_up_log(script_name)

DATA_DIR = Path(
    r"C:\Users\n740789\Documents\Projects_local\DataSets"
    r"\DATAFEED\ficheros_tratados\2025"
)

EXCEL_OUT = Path(r"C:\Users\n740789\Downloads\lost_issuers_2025.xlsx")
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

    for prev_file, next_file in zip(files, files[1:]):
        prev_month, next_month = month_tag(prev_file), month_tag(next_file)

        prev_df = load_df(prev_file)
        next_df = load_df(next_file)

        missing_ids = prev_df.index.difference(next_df.index)
        n_missing = len(missing_ids)

        logger.info(
            f"From {prev_month} to {next_month}, " f"{n_missing} issuer(s) were lost"
        )

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
        logger.info(f"Excel summary written to {EXCEL_OUT}")
    else:
        logger.info("No missing issuers detected; Excel file was not created.")


if __name__ == "__main__":
    main()
