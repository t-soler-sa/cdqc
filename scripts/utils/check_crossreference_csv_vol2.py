#!/usr/bin/env python
"""
check_crossreference_csv_vol2.py
Locate every row in the CSV that is not valid UTF-8 and print it.

• Any \xa0 (NBSP) or other non-UTF-8 byte is detected.
• Rows are decoded with Latin-1 for display (never fails) and NBSP bytes
  are replaced by the glyph '□' so you can see exactly where they are.
"""

from pathlib import Path

# ⇩  Update this path or pass it via sys.argv / argparse if you prefer
CSV_PATH = r"C:\Users\n740789\Documents\esg-sri-repos\clarity_data_quality_controls\excel_books\aladdin_data\crossreference\Aladdin_Clarity_Issuers_20250601.csv"


def bad_rows(path: str, expected_encoding: str = "utf-8"):
    """
    Yield (lineno, error, raw_line_bytes) for every line that fails to decode
    with the expected encoding.
    """
    with Path(path).open("rb") as fh:
        for lineno, raw in enumerate(fh, 1):  # 1-based line numbers
            try:
                raw.decode(expected_encoding)
            except UnicodeDecodeError as err:
                yield lineno, err, raw


def main():
    found = False
    for lineno, err, raw in bad_rows(CSV_PATH):
        found = True
        # Decode with Latin-1 so we can show the line without further errors
        # and replace NBSP (\xA0) with a visible square.
        pretty = (
            raw.decode("latin-1", errors="replace")
            .replace("\u00a0", "□")
            .rstrip("\n\r")
        )
        print(f"{lineno}: {pretty}")
    if not found:
        print("✅  No UTF-8 decoding problems detected.")


if __name__ == "__main__":
    main()
