import logging
import logging.config
import os
import json
from datetime import datetime


def set_up_log(log_name: str):
    """
    Sets up a logger using a JSON configuration file.

    The configuration file (logconfig.json) is expected at:
      <repo_root>/log/logconfig.json

    The log file for the RotatingFileHandler will be created at:
      <repo_root>/log/<DATE>_logs/<log_name>/<log_name>_<DATE>.log

    Parameters:
        log_name (str): Name identifier for the logger.

    Returns:
        logging.Logger: Configured logger.
    """
    # Format today's date as YYYYMMDD.
    DATE = datetime.now().strftime("%Y%m%d")

    # Determine the repository root:
    # Since this file is in <repo_root>/scripts/utils, go up three levels.
    repo_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    # Build the log directory path for the log file:
    log_file_dir = os.path.join(repo_dir, "log", f"{DATE}_logs", log_name)
    os.makedirs(log_file_dir, exist_ok=True)

    # Build the log file path:
    log_file = os.path.join(log_file_dir, f"{log_name}_{DATE}.log")

    # Path to the JSON logging configuration file:
    log_config_file = os.path.join(repo_dir, "log", "logconfig.json")

    # Load the logging configuration from the JSON file.
    with open(log_config_file, "r") as f:
        logging_config = json.load(f)

    # Update the file handler's filename with our computed log_file path.
    # (This assumes that the file handler is keyed as "file" in the JSON.)
    logging_config["handlers"]["file"]["filename"] = log_file

    # Apply the logging configuration.
    logging.config.dictConfig(logging_config)

    # Retrieve and return the logger with the specified name.
    logger = logging.getLogger(log_name)
    return logger
