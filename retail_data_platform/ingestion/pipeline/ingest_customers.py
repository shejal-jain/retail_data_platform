import os

from retail_data_platform.config.config import BASE_DATA_LAKE_PATH, BASE_SOURCE_PATH, CUSTOMERS
from retail_data_platform.ingestion.sources.customer_source import generate_customers_csv
from retail_data_platform.ingestion.loaders.file_loader import read_csv, save_json
from retail_data_platform.ingestion.utils.logger import get_logger

logger = get_logger("ingest_customers")


def build_source_path(run_id):
    """
    Build deterministic file path using run_id
    """
    file_name = f"customers_{run_id}.csv"
    return os.path.join(BASE_SOURCE_PATH, "customers", file_name)


def step_generate(source_file):
    try:
        # Prevent duplicate generation
        if os.path.exists(source_file):
            logger.info(f"File already exists, skipping generation: {source_file}")
            return

        logger.info(f"Generating CSV at {source_file}")
        generate_customers_csv(source_file, n=20)

    except Exception as e:
        logger.error(f"Generate step failed: {str(e)}")
        raise


def step_read(source_file):
    try:
        logger.info(f"Reading CSV from {source_file}")

        if not os.path.exists(source_file):
            raise FileNotFoundError(f"File not found: {source_file}")

        data = read_csv(source_file)
        logger.info(f"Read {len(data)} records")

        return data

    except Exception as e:
        logger.error(f"Read step failed: {str(e)}")
        raise


def step_save(data,run_id,logical_date):
    try:
        logger.info("Saving to data lake")

        file_path = save_json(
            data=data,
            base_path=BASE_DATA_LAKE_PATH,
            entity=CUSTOMERS,
            run_id=run_id,
            logical_date=logical_date
        )

        logger.info(f"Saved to {file_path}")

    except Exception as e:
        logger.error(f"Save step failed: {str(e)}")
        raise


def run_customers_ingestion(run_id, logical_date):
    logger.info(f"Customers ingestion started | run_id={run_id}")

    source_file = build_source_path(run_id)

    step_generate(source_file)
    data = step_read(source_file)
    step_save(data, run_id, logical_date)

    logger.info("Customers ingestion completed")