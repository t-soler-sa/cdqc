import pandas as pd
import json
from pathlib import Path


# this is the path of the csv file "C:\Users\n740789\Documents\pre-ovr-dashboard\src\assets\input\clarity_datafeed_without_ovr.csv"

csv_file_path = Path(
    r"C:\Users\n740789\Documents\pre-ovr-dashboard\src\assets\input\clarity_datafeed_without_ovr.csv"
)
output_json_path = Path(
    r"C:\Users\n740789\Documents\pre-ovr-dashboard\src\assets\input\clarity_datafeed_without_ovr.json"
)


def convert_large_csv_to_json(csv_path: Path, json_path: Path):
    df = pd.read_csv(csv_path, nrows=500)
    df.to_json(json_path, orient="records", force_ascii=False, indent=2)


# Example usage
convert_large_csv_to_json(csv_file_path, output_json_path)
