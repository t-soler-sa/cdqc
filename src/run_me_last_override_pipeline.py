#!/usr/bin/env python3
# wrap_run_me_last_override_pipeline.py
"""Run *scripts/run_datapipeline_override.py* with a fixed date.

Edit ARGS below, then press ▶︎ in VS Code.
"""

from pathlib import Path
import subprocess
import sys

# ─── CUSTOMISE ME ────────────────────────────────────────────────────────────
DATE = "202507"  # yyyymm
FLAGS = []  # this pipeline takes only the date, but keep for symmetry
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT = Path(__file__).parent.parent / "scripts" / "override_lists_impact_pipeline.py"
ARGS = [DATE, *FLAGS]

cmd = [sys.executable, str(SCRIPT), *ARGS]
print(">>>", " ".join(cmd))
result = subprocess.run(cmd, cwd=Path(__file__).parent)
sys.exit(result.returncode)
