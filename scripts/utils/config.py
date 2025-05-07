# config.py
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

from .get_date import get_date
from .set_up_log import set_up_log
from .get_output_dir import get_output_dir


def get_config(
    script_name: str = "default",
    interactive: bool = False,
    gen_output_dir: bool = False,
    output_dir_dated: bool = False,
    auto_date: bool = True,
    fixed_date: str = None,
) -> dict:
    """
    Set up and return common configuration settings.

    This function configures the logger, retrieves the date (in YYYYMM format),
    and sets up common file paths.

    Parameters:
        script_name (str): The name of the script.
        interactive (bool): Whether to allow interactive prompts for OUTPUT_DIR.
                            Defaults to True.
        gen_output_dir (bool): Whether to generate the output directory.
    Returns:
        dict: A configuration dictionary with various constants and paths.
    """
    # Initialize logger for the current script
    logger = set_up_log(script_name)

    if auto_date:
        # Use the get_date function to retrieve the date in YYYYMM format.
        DATE = get_date()  # Expected to return a valid date string in "YYYYMM" format.
    else:
        if fixed_date is None:
            logger.warning("fixed_date must be provided when get_date is False.")
            try:
                input_date = input("Please enter the date in YYYYMM format: ").strip()
                fixed_date = input_date
            except EOFError:
                logger.error("No input provided for date. Exiting.")
                sys.exit(1)
        else:
            # Validate the fixed_date format
            DATE = fixed_date

    YEAR = DATE[:4]
    date_obj = datetime.strptime(DATE, "%Y%m")
    prev_date_obj = date_obj - relativedelta(months=1)
    DATE_PREV = prev_date_obj.strftime("%Y%m")

    # Define common paths
    # REPO_DIR is derived relative to this script to get the repository's path.
    REPO_DIR = Path(r"C:\Users\n740789\Documents\clarity_data_quality_controls")
    # DATAFEED_DIR is taken from an enviroment variable (with a fallback if needed)
    DATAFEED_DIR = Path(r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED")
    EXCEL_BOOKS_DIR = REPO_DIR / "excel_books"
    SRI_DATA_DIR = EXCEL_BOOKS_DIR / "sri_data"
    ALADDIN_DATA_DIR = EXCEL_BOOKS_DIR / "aladdin_data"
    NASDAQ_DATA_DIR = EXCEL_BOOKS_DIR / "nasdaq_data"
    SUSTAINALYTICS_DATA_DIR = EXCEL_BOOKS_DIR / "sustainalytics_data"
    ESG_METRICS_MAP_DIR = SRI_DATA_DIR / "esg_metrics"

    # Define paths that are common for many scripts
    paths = {
        "CURRENT_DF_WOUTOVR_SEC_PATH": DATAFEED_DIR
        / "raw_dataset"
        / f"{YEAR}"
        / f"{DATE}01_Production"
        / f"{DATE}01_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv",
        "PRE_DF_WOVR_PATH": DATAFEED_DIR
        / "datafeeds_with_ovr"
        / f"{DATE_PREV}_df_issuer_level_with_ovr.csv",
        "NEW_DF_WOVR_PATH": DATAFEED_DIR
        / "datafeeds_with_ovr"
        / f"{DATE}_df_issuer_level_with_ovr.csv",
        "DF_WOVR_PATH_DIR": DATAFEED_DIR / "datafeeds_with_ovr",
        "CURRENT_DF_WOUTOVR_PATH": DATAFEED_DIR
        / "ficheros_tratados"
        / f"{YEAR}"
        / f"{DATE}01_Equities_feed_IssuerLevel_sinOVR.csv",
        "PROCESSED_DFS_WOUTOVR_PATH": DATAFEED_DIR / "ficheros_tratados" / f"{YEAR}",
        "CROSSREFERENCE_PATH": ALADDIN_DATA_DIR
        / "crossreference"
        / f"Aladdin_Clarity_Issuers_{DATE}01.csv",
        "BMK_PORTF_STR_PATH": ALADDIN_DATA_DIR
        / "bmk_portf_str"
        / f"{DATE}_strategies_snt world_portf_bmks.xlsx",
        "OVR_PATH": SRI_DATA_DIR / "overrides" / "overrides_db.xlsx",
        "COMMITTEE_PATH": REPO_DIR
        / "excel_books"
        / "sri_data"
        / "portfolios_committees"
        / "portfolio_lists.xlsx",
        "NASDAQ_DATA_PATH": NASDAQ_DATA_DIR / f"{DATE}_esg_nasdad_flag.xlsx",
        "SUSTAINALYTICS_DATA_PATH": SUSTAINALYTICS_DATA_DIR
        / f"{DATE}_sustainalytics_controversies.xlsx",
    }

    # If interactive  & generate output dir true, Determine OUTPUT_DIR
    # based on the script name, either using date if the structure of the output dir needs it...
    if gen_output_dir and output_dir_dated:

        OUTPUT_DIR = get_output_dir(
            script_name=script_name,
            sri_data_dir=SRI_DATA_DIR,
            interactive=interactive,
            dated=True,
            dir_date=DATE,
            logger=logger,
        )
    # ... or without date if the user does not need structure of the output to have it.
    elif gen_output_dir:
        OUTPUT_DIR = get_output_dir(
            script_name=script_name,
            sri_data_dir=SRI_DATA_DIR,
            interactive=interactive,
            logger=logger,
        )
    else:
        OUTPUT_DIR = None

    # Build and return the configuration dictionary
    config = {
        "logger": logger,
        "DATE": DATE,
        "YEAR": YEAR,
        "DATE_PREV": DATE_PREV,
        "REPO_DIR": REPO_DIR,
        "DATAFEED_DIR": DATAFEED_DIR,
        "SRI_DATA_DIR": SRI_DATA_DIR,
        "EXCEL_BOOKS_DIR": EXCEL_BOOKS_DIR,
        "ALADDIN_DATA_DIR": ALADDIN_DATA_DIR,
        "NASDAQ_DATA_DIR": NASDAQ_DATA_DIR,
        "SUSTAINALYTICS_DATA_DIR": SUSTAINALYTICS_DATA_DIR,
        "paths": paths,
        "OUTPUT_DIR": OUTPUT_DIR,
        "ESG_METRICS_MAP_DIR": ESG_METRICS_MAP_DIR,
    }
    return config
