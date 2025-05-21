import re
from pathlib import Path


def remove_duplicate_warnings(input_path, output_path):
    print(f"Checking path: {input_path}")
    if not input_path.exists():
        print("❌ File does not exist!")
        return
    seen = set()
    pattern = re.compile(r"WARNING\s*-\s*(.*)")

    with open(input_path, "r", encoding="utf-8") as infile, open(
        output_path, "w", encoding="utf-8"
    ) as outfile:
        for i, line in enumerate(infile, 1):
            match = pattern.search(line)
            if match:
                message = match.group(1).strip()
                if message not in seen:
                    outfile.write(message + "\n")
                    seen.add(message)
            else:
                print("  ❌ No match for WARNING")


if __name__ == "__main__":
    path_dir = Path(
        r"C:\Users\n740789\Documents\clarity_data_quality_controls\log\20250521_logs\01-generate-ovr-lists"
    )
    path_in = path_dir / "01-generate-ovr-lists_20250521.log"
    path_out = path_dir / "01-generate-ovr-filtered.log"
    remove_duplicate_warnings(path_in, path_out)
