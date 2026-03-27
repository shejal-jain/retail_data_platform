import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

def save_processed(data, timestamp):
    try:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        
        processed_file_path = PROCESSED_DIR / f"news_clean_{timestamp}.json"

        logger.info(f"Saving processed data to {processed_file_path}")

        with open(processed_file_path, "w") as file:
            json.dump(data, file)

        logger.info("Processed data saved successfully")

        return processed_file_path

    except Exception as e:
        logger.error(f"Error saving processed data: {str(e)}")
        raise