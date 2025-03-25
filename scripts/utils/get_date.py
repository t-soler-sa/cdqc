import argparse
import logging
import sys
from datetime import datetime

# Module-level logger
logger = logging.getLogger(__name__)


def get_date() -> str:
    """
    Process command-line arguments or prompt the user for a date in YYYYMM format.

    Returns:
        str: A valid date string in YYYYMM format.
    """
    parser = argparse.ArgumentParser(description="Process data for a specific date.")
    parser.add_argument("date", nargs="?", help="Date in YYYYMM format")
    parser.add_argument("--date", dest="date_flag", help="Date in YYYYMM format")

    # If running in an IPython kernel (like in a notebook), ignore unknown arguments.
    if "ipykernel_launcher" in sys.argv[0]:
        args, _ = parser.parse_known_args()
    else:
        args = parser.parse_args()

    if args.date and validate_date(args.date):
        logger.info("Date provided as command-line argument: %s", args.date)
        return args.date
    elif args.date_flag and validate_date(args.date_flag):
        logger.info("Date provided with --date flag: %s", args.date_flag)
        return args.date_flag
    else:
        # Prompt the user until a valid date is provided.
        while True:
            date_input = input("Enter the date in YYYYMM format: ")
            if validate_date(date_input):
                return date_input
            print("Invalid date format. Please use YYYYMM.")


def validate_date(date_string: str) -> bool:
    """
    Validate if the date string is in the YYYYMM format.

    Args:
        date_string (str): The date string to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    try:
        datetime.strptime(date_string, "%Y%m")
        logger.info("Date format is valid. Date set to %s.", date_string)
        return True
    except ValueError:
        logger.warning("Invalid date format entered: %s", date_string)
        return False


def main():
    date = get_date()
    logger.info(f"Date provided: {date}")


if __name__ == "__main__":
    main()
