"""
This module contains the logging configuration for the project.

Every logger used is created in this module and can be imported in other modules where they are used.
"""

import logging
import os
import dotenv

dotenv.load_dotenv()

from politifact_scraping.utils import load_env_var

LOG_PATH = f"{load_env_var('LOCAL_DB')}/logs"

if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)
    logging.warning(f"Logging directory {LOG_PATH} did not exist. It has been automatically created.")


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with the specified name and log file.

    :param name: Name of the logger.
    :type name: str
    :param log_file: Path to the log file.
    :type log_file: str
    :param level: Logging level, defaults to logging.INFO
    :type level: int, optional

    :return: The logger object.
    :rtype: logging.Logger
    """
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


# Logger for generic messagesLogs into the file `logs/generic.log`.
LOG_GENERIC = setup_logger("generic", f"{LOG_PATH}/generic.log")

# Logger for messages related to the scraping to Politifact website. Logs into the file `logs/politifact_scraping.log`.
LOG_POLITIFACT_SCRAPING = setup_logger("politifact_scraping", f"{LOG_PATH}/politifact_scraping.log")

# Logger for messages related to MongoDB databases
LOG_MONGODB = setup_logger("deleted_mongodb_docs", f"{LOG_PATH}/mongodb.log")

# Logger for messages related to the deleting documents from the MongoDB database. Logs into the file `logs/deleted_mongodb_docs.log`.
LOG_DELETED_MONGODB_DOCS = setup_logger("deleted_mongodb_docs", f"{LOG_PATH}/deleted_mongodb_docs.log")
