#!/usr/bin/env python3
"""
run_datapipeline_ovr.py – Windows/Git-Bash friendly replacement for run_datapipeline.sh

Usage:
    python run_datapipeline.py 202411
"""

from pathlib import Path
import re
import subprocess
import sys
import time


def main() -> None:
    start_time = time.time()

    # ── 1. Parameter handling ────────────────────────────────────────────────
    if len(sys.argv) < 2:
        sys.exit("Please provide a date parameter (format: yyyymm)")

    date_arg = sys.argv[1]
    if not re.fullmatch(r"\d{6}", date_arg):
        sys.exit("Invalid date format. Use yyyymm (e.g. 202411)")

    # ── 2. Resolve project root ──────────────────────────────────────────────
    base_dir = Path(__file__).resolve().parent.parent
    print(f"Base directory: {base_dir}")

    # ── 3. Pipeline definition ──────────────────────────────────────────────
    script_order = [
        "_01_generate_ovr_lists.py",
        "_02_apply_ovr.py",
        "_03_noncompliance_analysis.py",
        "_04_impact_analysis.py",
    ]

    # ── 4. Execute each stage ───────────────────────────────────────────────
    for script in script_order:
        module = f"scripts.{Path(script).stem}"
        print(f"Running {script}")
        try:
            subprocess.run(
                [sys.executable, "-m", module, "--date", date_arg],
                cwd=base_dir,
                check=True,
            )
        except subprocess.CalledProcessError as err:
            sys.exit(f"Error while running {script} (exit code {err.returncode})")

    # ── 5. Elapsed-time summary ─────────────────────────────────────────────
    total_sec = round(time.time() - start_time)
    minutes, seconds = divmod(total_sec, 60)
    print("All Python scripts completed successfully")
    print(f"Time: {minutes} min, {seconds} seconds.")


if __name__ == "__main__":  # pragma: no cover
    main()
