#!/usr/bin/env python
"""
find_wide_rows.py  –  dump any CSV row whose field-count exceeds 6.

Usage:
    python find_wide_rows.py myfile.csv > bad_rows.csv
"""

import sys, csv, pathlib

EXPECTED_COLS = 6  # tweak if the spec ever changes
DELIM = ","  # tweak if you use another separator
QUOTECHAR = '"'  # tweak if your file uses a different quote


def main(path: str):
    path = pathlib.Path(path)

    with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=DELIM, quotechar=QUOTECHAR)
        header = next(reader, None)  # keep header out of the results
        for lineno, row in enumerate(reader, start=2):  # data starts at line 2
            if len(row) > EXPECTED_COLS:
                # write: line number • field-count • raw line
                raw = DELIM.join(row).replace("\n", r"\n")
                print(f'{lineno},{len(row)},"{raw}"')


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: find_wide_rows.py file.csv > bad_rows.csv")
    main(sys.argv[1])
