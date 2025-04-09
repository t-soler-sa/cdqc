import json
from pathlib import Path

JSON_FILE = Path(
    r"C:\Users\n740789\Documents\clarity_data_quality_controls\json_files\strategies.json"
)


def update_metrics_affecting(json_data):
    for strategy_key, strategy_value in json_data.get("strategies", {}).items():
        metrics_affecting = set()

        excluded = strategy_value.get("outcome", {}).get("excluded", {})

        for condition in excluded.values():
            metric_id = condition.get("metric_id")
            if metric_id:
                metrics_affecting.add(metric_id)

        strategy_value["metrics_affecting"] = sorted(metrics_affecting)

    return json_data


# Load your JSON file
with JSON_FILE.open("r") as file:
    data = json.load(file)

# Update the metrics_affecting lists
data_updated = update_metrics_affecting(data)

# Save the modified JSON back to the same file
with JSON_FILE.open("w") as file:
    json.dump(data_updated, file, indent=2)

print("Updated JSON saved.")
