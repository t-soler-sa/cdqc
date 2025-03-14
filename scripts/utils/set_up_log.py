import logging
import os
from datetime import datetime


def set_up_log(log_name: str):
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "log")
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(
                os.path.join(
                    log_dir,
                    f'{log_name}_{datetime.now().strftime("%Y%m%d")}.log',
                )
            ),
            logging.StreamHandler(),
        ],
    )

    logger = logging.getLogger(f"{log_name}")
    return logger
