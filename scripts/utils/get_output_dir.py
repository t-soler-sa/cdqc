# get_output_dir.py
from pathlib import Path


def get_output_dir(
    script_name: str,
    sri_data_dir: Path,
    interactive: bool = True,
    dated: bool = False,
    dir_date: str = None,
    logger: object = None,
) -> Path:
    """
    Determines the output directory path based on the script name, user interaction,
    and optional date-based structuring, then creates the directory if it does not exist.

    Parameters:
        script_name (str): Name of the calling script used to determine the directory mapping.
        sri_data_dir (Path): Base directory where output subdirectories will be created.
        interactive (bool): If True, prompts the user to confirm or choose the directory name when
                            not found in the predefined mapping. Defaults to True.
        dated (bool): If True, appends a year and year-month structure to the directory path using dir_date.
                      Defaults to False.
        dir_date (str): Required if `dated` is True. A string in 'YYYYMM' format specifying the year and month.
        logger (object): Logger instance used to log informational and error messages throughout the process.

    Returns:
        Path: A pathlib.Path object pointing to the created or existing output directory.

    Raises:
        ValueError: If `dated` is True but `dir_date` is missing or not in the 'YYYYMM' format.
        Exception: If directory creation fails due to underlying filesystem or permission errors.

    Behavior:
        - Uses a predefined mapping of script names to standardized directory names.
        - In non-interactive mode, defaults to using the lowercased script name when no mapping exists.
        - In interactive mode, prompts the user to confirm or select a directory name when no mapping exists.
        - When `dated` is True, further nests the directory within 'year/yearmonth' subdirectories.
        - Ensures the target directory exists by creating it if necessary.
    """
    mapping = {
        "zombie-killer": "zombie_list",
        "pre_ovr_analysis": "pre_ovr_analysis",
        "00_preovr_analysis_str_level": "pre_ovr_analysis",
        "overrides": "overrides",
        "noncompliance": "noncompliance",
        "noncomplieance-analysis": "noncompliance",
        "impact_analysis": "impact_analysis",
    }

    script_lower = script_name.lower().strip()
    if script_lower in mapping:
        dir_name = mapping[script_lower]
    elif not interactive:
        # In non-interactive mode, default to script_lower
        logger.info(
            f"Non-interactive mode enabled. Using script name {script_lower} as directory name."
        )
        dir_name = script_lower
    else:
        logger.info("Interactive mode enabled. Prompting for directory name.")
        # Ask user if it's okay to create a new directory
        user_response = (
            input(
                f"No dir was found. A new dir will be created with the name '{script_lower}'. Are you OK with that? (y/n): "
            )
            .strip()
            .lower()
        )

        if user_response in ("y", "yes"):
            dir_name = script_lower
            logger.info(f"Creating new directory: {dir_name}")
        else:
            options = [
                "pre_ovr_analysis",
                "zombie_list",
                "overrides",
                "noncompliance",
                "impact_analysis",
            ]
            print("Please choose one of the following options:")
            for idx, option in enumerate(options, 1):
                print(f"{idx} - {option}")

            while True:
                selection = input("Enter a number (1-5): ").strip()
                if selection in [str(i) for i in range(1, len(options) + 1)]:
                    dir_name = options[int(selection) - 1]
                    logger.info(f"Creating new directory: {dir_name}")
                    break
                else:
                    print("Invalid selection. Please enter a number between 1 and 5.")

    if dated:
        logger.info("Dated mode enabled. Using dir_date for directory name.")
        from datetime import datetime

        # check dir_date is in YYYYMM format
        if dir_date is None:
            logger.error("dir_date must be provided when dated is True.")
            raise ValueError("dir_date must be provided when dated is True.")
        elif not isinstance(dir_date, str) or len(dir_date) != 6:
            logger.error("dir_date must be a string in YYYYMM format.")
            raise ValueError("dir_date must be a string in YYYYMM format.")
        try:
            datetime.strptime(dir_date, "%Y%m")
            year_month = dir_date
            year = dir_date[:4]
            output_dir = sri_data_dir / dir_name / f"{year}" / f"{year_month}"
            logger.info(f"Creating dated directory: {output_dir}")
        except ValueError:
            logger.error("dir_date must be a string in YYYYMM format.")
            raise ValueError("dir_date must be a string in YYYYMM format.")

    else:
        output_dir = sri_data_dir / dir_name
        logger.info(f"Creating non-dated directory: {output_dir}")

    try:
        logger.info(f"Creating directory {output_dir} if it does not exists")
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating directory {output_dir}: {e}")
        raise

    logger.info(f"Output directory for script {script_name} is set to: {output_dir}")
    return output_dir
