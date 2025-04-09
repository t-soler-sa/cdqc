import pandas as pd
import json
from pathlib import Path

# Paths to your CSV and JSON files
CSV_FILE = Path(
    r"C:\Users\n740789\Documents\clarity_data_quality_controls\clarity_strategies\20250407_esg_metrics_table.csv"
)
JSON_FILE = Path(
    r"C:\Users\n740789\Documents\clarity_data_quality_controls\json_files\strategies.json"
)
OUTPUT_JSON_FILE = "updated.json"

# Load CSV and create mapping
df = pd.read_csv(CSV_FILE)
metric_map = dict(zip(df["metric_name"], df["metric_id"]))

# Load JSON
with open(JSON_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)


# Improved recursive function
def recursive_update(obj, metric_map):
    if isinstance(obj, dict):
        for key, value in obj.items():
            # Update if key matches and metric_id exists in the nested dictionary
            if key in metric_map and isinstance(value, dict) and "metric_id" in value:
                old_value = value["metric_id"]
                value["metric_id"] = metric_map[key]
                print(f"Updated {key}: {old_value} -> {value['metric_id']}")
            # Recursive call for nested structures
            recursive_update(value, metric_map)
    elif isinstance(obj, list):
        for item in obj:
            recursive_update(item, metric_map)


# Update JSON
total_updates = recursive_update(data, metric_map)

# Save updated JSON
with open(OUTPUT_JSON_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"JSON file successfully updated and saved as {OUTPUT_JSON_FILE}")
