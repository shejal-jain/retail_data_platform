# ingestion/utils/logger.py

import logging
import os


def get_logger(name):
    """
    Creates and returns a logger

    Parameters:
    name (str): name of the logger (usually file/module name)
    """

    # Create logger object
    logger = logging.getLogger(name)

    # Set logging level (INFO means capture INFO, WARNING, ERROR)
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs
    if not logger.handlers:

        # Create logs folder if not exists
        os.makedirs("logs", exist_ok=True)

        # File handler (writes logs to file)
        file_handler = logging.FileHandler(f"logs/{name}.log")

        # Console handler (prints logs to terminal)
        console_handler = logging.StreamHandler()

        # Define log format
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        # Apply format
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger