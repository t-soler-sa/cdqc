# config.py

from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

from .get_date import get_date
from .set_up_log import set_up_log
from .get_output_dir import get_output_dir


def get_config(
    script_name: str = "default", interactive: bool = True, gen_output_dir: bool = True
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

    # Get the date in YYYYMM format.
    DATE = get_date()  # Expected to return a valid date string in "YYYYMM" format.
    YEAR = DATE[:4]
    date_obj = datetime.strptime(DATE, "%Y%m")
    prev_date_obj = date_obj - relativedelta(months=1)
    DATE_PREV = prev_date_obj.strftime("%Y%m")

    # Define common paths
    # REPO_DIR is derived relative to this script to get the repository's path.
    REPO_DIR = Path(r"C:\Users\n740789\Documents\clarity_data_quality_controls")
    # DATAFEED_DIR is taken from an enviroment variable (with a fallback if needed)
    DATAFEED_DIR = Path(r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED")
    SRI_DATA_DIR = REPO_DIR / "excel_books" / "sri_data"

    # Define paths that are common for many scripts
    paths = {
        "RAW_DF_WOUT_OVR_PATH": DATAFEED_DIR
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
        "CROSSREFERENCE_PATH": REPO_DIR
        / "excel_books"
        / "aladdin_data"
        / "crossreference"
        / f"Aladdin_Clarity_Issuers_{DATE}01.csv",
        "BMK_PORTF_STR_PATH": REPO_DIR
        / "excel_books"
        / "aladdin_data"
        / "bmk_portf_str"
        / f"{DATE}_strategies_snt world_portf_bmks.xlsx",
        "OVR_PATH": SRI_DATA_DIR / "overrides" / "overrides_db.xlsx",
        "COMMITTEE_PATH": REPO_DIR
        / "excel_books"
        / "sri_data"
        / "portfolios_committees"
        / "portfolio_lists.xlsx",
    }

    if gen_output_dir:
        # Determine OUTPUT_DIR based on the script name, passing the interactive flag.
        OUTPUT_DIR = get_output_dir(script_name, SRI_DATA_DIR, interactive=interactive)
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
        "paths": paths,
        "OUTPUT_DIR": OUTPUT_DIR,
    }
    return config
