import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Get Project Root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

def save_raw(data, timestamp):
    try:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        
        raw_file_path = RAW_DIR / f"news_{timestamp}.json"

        logger.info(f"Saving raw data to {raw_file_path}")

        with open(raw_file_path, "w") as file:
            json.dump(data, file)

        logger.info("Raw data saved successfully")

        return raw_file_path

    except Exception as e:
        logger.error(f"Error saving raw data: {str(e)}")
        raise