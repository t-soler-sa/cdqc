import re
from pathlib import Path
from datetime import datetime

# import logging
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def remove_duplicate_warnings(input_path, output_path):
    logger.info(f"Checking path: {input_path}")
    if not input_path.exists():
        logger.warning("❌ File does not exist!")
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
                    # Post-process message
                    marker = "in <module>()] - "
                    if marker in message:
                        message = message.split(marker, 1)[1].strip()
                    outfile.write(message + "\n")
                    seen.add(message)
            else:
                pass


def main():
    # today date in yyyymmdd format
    today = datetime.now().strftime("%Y%m%d")
    path_dir = Path(
        rf"C:\Users\n740789\Documents\clarity_data_quality_controls\log\{today}_logs\01-generate-ovr-lists"
    )
    path_in = path_dir / f"01-generate-ovr-lists_{today}.log"
    logger.info(f"Filtering log: {path_in}")
    path_out = path_dir / "01-generate-ovr-filtered.log"
    remove_duplicate_warnings(path_in, path_out)
    logger.info(f"Filtered log saved to: {path_out}")


if __name__ == "__main__":
    main()
