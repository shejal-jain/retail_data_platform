import json
import os
import logging
from datetime import datetime, UTC
from retail_data_platform.config.config import ALLOWED_ENTITIES
from retail_data_platform.ingestion.utils.logger import get_logger

logger = get_logger("file_loader")


def save_json(data, base_path, entity, run_id, logical_date):
    """
    Saves data into data lake in partitioned structure
    """

    # Validate entity
    if entity not in ALLOWED_ENTITIES:
        raise ValueError(f"Invalid entity: {entity}")

    # Partition by date
    date_str = logical_date.strftime("%Y-%m-%d")

    folder_path = os.path.join(base_path, entity, date_str)
    os.makedirs(folder_path, exist_ok=True)

    file_name = f"{entity}_{run_id}.json"
    file_path = os.path.join(folder_path, file_name)

    if os.path.exists(file_path):
        logger.warning(f"Overwriting existing file: {file_path}")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    return file_path


def read_csv(file_path):
    """
    Reads CSV and returns list of dictionaries
    """
    import csv

    data = []

    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    return data