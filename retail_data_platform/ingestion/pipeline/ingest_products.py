import os

from retail_data_platform.config.config import BASE_DATA_LAKE_PATH, BASE_SOURCE_PATH, PRODUCTS
from retail_data_platform.ingestion.sources.product_source import generate_products_csv
from retail_data_platform.ingestion.loaders.file_loader import read_csv, save_json
from retail_data_platform.ingestion.utils.logger import get_logger

logger = get_logger("ingest_products")


def build_source_path(run_id):
    file_name = f"products_{run_id}.csv"
    return os.path.join(BASE_SOURCE_PATH, "products", file_name)


def step_generate(source_file):
    try:
        if os.path.exists(source_file):
            logger.info(f"Skipping generation: {source_file}")
            return

        generate_products_csv(source_file, n=15)

    except Exception as e:
        logger.error(f"Generate failed: {str(e)}")
        raise


def step_read(source_file):
    try:
        return read_csv(source_file)
    except Exception as e:
        logger.error(f"Read failed: {str(e)}")
        raise


def step_save(data, run_id, logical_date):
    try:
        save_json(data, BASE_DATA_LAKE_PATH, PRODUCTS, run_id, logical_date)
    except Exception as e:
        logger.error(f"Save failed: {str(e)}")
        raise


def run_products_ingestion(run_id, logical_date):
    source_file = build_source_path(run_id)

    step_generate(source_file)
    data = step_read(source_file)
    step_save(data,run_id, logical_date)