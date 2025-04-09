import os
import pandas as pd


def convert_csvs_to_json(folder_path):
    if not os.path.isdir(folder_path):
        print(f"❌ Provided path is not a directory: {folder_path}")
        return

    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    if not files:
        print("📂 No CSV files found in the directory.")
        return

    for file_name in files:
        csv_path = os.path.join(folder_path, file_name)
        json_name = file_name.replace(".csv", ".json")
        json_path = os.path.join(folder_path, json_name)

        try:
            print(f"🔄 Converting: {file_name} → {json_name}")
            df = pd.read_csv(csv_path)
            df.to_json(json_path, orient="records", force_ascii=False, indent=2)

            os.remove(csv_path)
            print(f"✅ Converted and deleted: {file_name}")
        except Exception as e:
            print(f"❌ Failed to convert {file_name}: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python csv_to_json_cleaner.py <folder_path>")
    else:
        convert_csvs_to_json(sys.argv[1])
