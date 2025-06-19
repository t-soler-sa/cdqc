#!/usr/bin/env python3
# wrap_run_me_first_pre_override_pipeline.py
"""Run *scripts/run_pre_ovr_pipeline.py* with canned arguments.

Edit ARGS below, then press ▶︎ in VS Code.
"""

from pathlib import Path
import subprocess
import sys

# ─── CUSTOMISE ME ─────────────────────────────────────────────────────────────────────────────────────
DATE = "202507"  # yyyymm
# Possible flags:  simple • zombie • only_preovr • no_dups - either only_preovr or no_dups, not both
FLAGS = [
    "only_preovr"
]  # this way the pipeline will skip all the scripts except the last one
# ──────────────────────────────────────────────────────────────────────────────────────────────────────

SCRIPT = Path(__file__).parent.parent / "scripts" / "pre_ovr_pipeline.py"
ARGS = [DATE, *FLAGS]

cmd = [sys.executable, str(SCRIPT), *ARGS]
print(">>>", " ".join(cmd))
result = subprocess.run(cmd, cwd=Path(__file__).parent)
sys.exit(result.returncode)
