import argparse
from datetime import datetime


def get_date() -> str:
    """
    Process command-line arguments or prompt the user for a date in YYYYMM format.

    Returns:
        str: A valid date string in YYYYMM format.
    """
    parser = argparse.ArgumentParser(description="Process data for a specific date.")
    parser.add_argument("date", nargs="?", help="Date in YYYYMM format")
    parser.add_argument("--date", dest="date_flag", help="Date in YYYYMM format")
    args = parser.parse_args()

    if args.date and validate_date(args.date):
        return args.date
    elif args.date_flag and validate_date(args.date_flag):
        return args.date_flag
    else:
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
        return True
    except ValueError:
        return False


def main():
    date = get_date()
    print(f"Date provided: {date}")


if __name__ == "__main__":
    main()
