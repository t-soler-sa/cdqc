#!/usr/bin/env python
"""
check_crossreference_csv_vol2_save_bad_rows.py
Scan a CSV for non-UTF-8 lines and write those lines (plus header) to a
separate CSV.

• Keeps the file’s original delimiter and quoting untouched.
• Fails fast if the destination already exists (to avoid overwriting).
• Prints a short summary when done.
"""

from pathlib import Path
import sys

# ---------------------------------------------------------------------------
# ⇩ Edit these two paths or pass them on the command line (src dest)
SRC_PATH = r"C:\Users\n740789\Documents\esg-sri-repos\clarity_data_quality_controls\excel_books\aladdin_data\crossreference\Aladdin_Clarity_Issuers_20250601.csv"
DEST_PATH = r"C:\Users\n740789\Documents\esg-sri-repos\clarity_data_quality_controls\excel_books\aladdin_data\crossreference\Aladdin_Clarity_Issuers_20250601_bad_rows.csv"
# ---------------------------------------------------------------------------


def save_bad_rows(src: str, dest: str, expected_encoding: str = "utf-8") -> int:
    src_p = Path(src)
    dest_p = Path(dest)

    if dest_p.exists():
        raise FileExistsError(
            f"{dest_p} already exists – pick another name or delete it."
        )

    bad_count = 0
    with src_p.open("rb") as fin, dest_p.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        header_raw = fin.readline()  # keep header
        fout.write(header_raw.decode("latin-1"))

        for lineno, raw in enumerate(fin, start=2):  # start=2 because header is line 1
            try:
                raw.decode(expected_encoding)
            except UnicodeDecodeError:
                bad_count += 1
                # write line exactly as in source, decoding with latin-1 never fails
                fout.write(raw.decode("latin-1"))

    return bad_count


if __name__ == "__main__":
    # Allow:  python save_bad_rows.py [source.csv] [dest.csv]
    if len(sys.argv) in (2, 3):
        SRC_PATH = sys.argv[1]
        if len(sys.argv) == 3:
            DEST_PATH = sys.argv[2]

    n = save_bad_rows(SRC_PATH, DEST_PATH)
    print(f"✔  Saved {n} non-UTF-8 row(s) to {DEST_PATH}")
