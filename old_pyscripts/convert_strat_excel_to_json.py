import pandas as pd
from pathlib import Path
import json


def excel_to_json(excel_file: Path, sheet_names: list) -> dict:
    import re

    strategies = {}
    for sheet in sheet_names:
        tmp_df = pd.read_excel(excel_file, sheet_name=sheet, header=None, nrows=2)
        strategy_description = str(tmp_df.iloc[1, 1])
        df = pd.read_excel(excel_file, sheet_name=sheet, header=3)
        print(f"Sheet '{sheet}' shape: {df.shape}")
        if "Topic" in df.columns:
            df = df.drop(columns=["Topic"])
        strategy_data = {
            "description": strategy_description,
            "metrics_affecting": [],
            "outcome": {"excluded": {}, "flag": {}},
        }
        for idx, row in df.iterrows():
            # Use the value from "Threshold/Rule" for the rule key, processed accordingly.
            original_condition_value = str(row.get("Threshold/Rule", "")).strip()
            rule_key = original_condition_value.lower().replace(" ", "_")

            # Process the "Threshold" column to extract condition and threshold.
            raw_threshold = str(row.get("Threshold", "")).strip()
            if raw_threshold.upper() == "ANY":
                final_condition = "any(filter_value)"
                final_threshold = "any(filter_value)=TRUE"
            else:
                if raw_threshold:
                    final_condition = raw_threshold[0]
                    remainder = raw_threshold[1:].strip()
                    if "OR" in remainder.upper():
                        parts = re.split(r"\s+OR\s+", remainder, flags=re.IGNORECASE)
                        final_threshold = [
                            part.strip() for part in parts if part.strip()
                        ]
                    else:
                        final_threshold = remainder
                else:
                    final_condition = ""
                    final_threshold = ""

            rule_type = row.get("Type of Exclusion", "")
            result = row.get("Result", "")
            rule = {
                "metric_sk": "",
                "type": rule_type,
                "condition": final_condition,
                "threshold": final_threshold,
                "result": result,
            }
            section = "flag" if str(result).strip().upper() == "FLAG" else "excluded"
            strategy_data["outcome"][section][rule_key] = rule
        strategies[sheet] = strategy_data
    return {"strategies": strategies}


# -----------------------
# Example usage:
# -----------------------

excel_path = Path(r"C:\Users\n740789\Downloads\042025_Estrategias_Mandatos_ISR.xlsx")


sheets_to_process = [
    "STR001",
    "STR002",
    "STR003",
    "STR004",
    "STR005",
    "STR006",
    "STR_SFDR8_AEC",
    "CS001",
    "CS002",
]
#
result_json = excel_to_json(excel_path, sheets_to_process)


# Define the output file path
output_file = Path(
    r"C:\Users\n740789\Documents\clarity_data_quality_controls\json_files\strategies_beta_beta.json"
)

# Save the JSON dictionary to the output file with proper formatting
with output_file.open("w", encoding="utf-8") as f:
    json.dump(result_json, f, indent=4)
