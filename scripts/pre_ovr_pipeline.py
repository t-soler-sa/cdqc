#!/usr/bin/env python3
"""
run_pre_ovr_pipeline.py — Windows / Git-Bash friendly replacement for run_pre_ovr_pipeline.sh

Usage
-----
    python run_pre_ovr_pipeline.py 202411 [simple] [zombie] [only_preovr | no_dups]

Flags (optional, case-sensitive)
--------------------------------
    simple        Produce a simplified override analysis
    zombie        Produce a zombie analysis
    only_preovr   Run ONLY _00_preovr_analysis.py
    no_dups       Skip utils/remove_duplicates.py

Notes
-----
* `only_preovr` and `no_dups` are **mutually exclusive**.
* The script assumes your “real” pipeline code lives in the package
  `scripts`, e.g. `scripts.utils.remove_duplicates`.
* If you run this inside an activated virtual-env, `sys.executable`
  automatically points at the right interpreter—no explicit “activate” step
  is necessary.
"""

import re
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple


def parse_args(argv: List[str]) -> Tuple[str, List[str], str, str]:
    """Validate CLI args and return (date, scripts, simple_flag, zombie_flag)."""
    if not argv:
        sys.exit("Please provide a date parameter (format: yyyymm)")

    date_arg = argv[0]
    if not re.fullmatch(r"\d{6}", date_arg):
        sys.exit("Invalid date format. Use yyyymm (e.g. 202411)")

    # Defaults replicate the Bash script
    scripts = [
        "utils/remove_duplicates.py",
        "utils/brs_issuer_data_to_csv.py",
        "utils/update_ovr_db_active_col.py",
        "_00_preovr_analysis.py",
    ]
    simple_flag = ""
    zombie_flag = ""

    remaining = argv[1:]
    valid_opts = {"simple", "zombie", "only_preovr", "no_dups"}

    for opt in remaining:
        if opt not in valid_opts:
            sys.exit(
                f"Unknown argument: {opt}\n"
                "Valid options after the date are: 'simple', 'zombie', "
                "'only_preovr' or 'no_dups' – but not only_preovr and no_dups "
                "at the same time"
            )

    if {"only_preovr", "no_dups"} <= set(remaining):
        sys.exit("Cannot combine only_preovr and no_dups")

    if "simple" in remaining:
        print(
            "Simple parameter provided! Simplified override analysis will be generated"
        )
        simple_flag = "--simple"

    if "zombie" in remaining:
        print("Zombie parameter provided! Zombie analysis will be generated")
        zombie_flag = "--zombie"

    if "only_preovr" in remaining:
        print("Only pre-override analysis will be generated")
        scripts = ["_00_preovr_analysis.py"]
    elif "no_dups" in remaining:
        print("We will skip the remove-duplicates script!")
        scripts = [
            "utils/brs_issuer_data_to_csv.py",
            "utils/update_ovr_db_active_col.py",
            "_00_preovr_analysis.py",
        ]

    return date_arg, scripts, simple_flag, zombie_flag


def main() -> None:
    start_time = time.time()

    date_arg, scripts, simple_flag, zombie_flag = parse_args(sys.argv[1:])

    base_dir = Path(__file__).resolve().parent.parent
    print(f"Base directory: {base_dir}")

    for script in scripts:
        module_path = (
            f"scripts.{Path(script).with_suffix('').as_posix().replace('/', '.')}"
        )
        print(f"Running {module_path}")

        cmd = [sys.executable, "-m", module_path]

        if module_path.endswith("_00_preovr_analysis"):
            if simple_flag:
                cmd.append(simple_flag)
            if zombie_flag:
                cmd.append(zombie_flag)

        cmd.extend(["--date", date_arg])

        print("Command:", " ".join(cmd))
        result = subprocess.run(cmd, cwd=base_dir)

        if result.returncode != 0:
            sys.exit(
                f"Error occurred while running {module_path} "
                f"(exit code {result.returncode})"
            )

    # ─────────────────────────────────────────────────────────────────────────
    print("All Python scripts completed successfully")
    print("Pre-override data pipeline completed successfully. Pre-ovr-analysis ready.")

    total_sec = round(time.time() - start_time)
    minutes, seconds = divmod(total_sec, 60)
    print(f"Time: {minutes} min, {seconds} seconds.")


if __name__ == "__main__":
    main()
