import os

from retail_data_platform.config.config import BASE_DATA_LAKE_PATH, BASE_SOURCE_PATH, PRODUCTS
from retail_data_platform.ingestion.sources.product_source import generate_products_csv
from retail_data_platform.ingestion.loaders.file_loader import read_csv, save_json
from retail_data_platform.ingestion.utils.logger import get_logger

logger = get_logger("ingest_products")


def build_source_path(run_id):
    file_name = f"products_{run_id}.csv"
    return os.path.join(BASE_SOURCE_PATH, "products", file_name)


def step_generate(source_file, run_id):
    logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] START")

    try:
        if os.path.exists(source_file):
            logger.info(f"[RUN_ID={run_id}] File exists, skipping: {source_file}")
            logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] SKIPPED")
            return

        logger.info(f"[RUN_ID={run_id}] Generating CSV at {source_file}")
        generate_products_csv(source_file, n=20)

        logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] SUCCESS")

    except Exception as e:
        logger.error(f"[RUN_ID={run_id}] [STEP: GENERATE] FAILED: {str(e)}")
        raise


def step_read(source_file, run_id, logical_date):
    logger.info(f"[RUN_ID={run_id}] [STEP: READ] START")

    try:
        data = read_csv(source_file)

        staging_path = save_json(
            data=data,
            base_path=BASE_DATA_LAKE_PATH,
            entity=f"{PRODUCTS}_staging",
            run_id=run_id,
            logical_date=logical_date
        )

        logger.info(f"[RUN_ID={run_id}] Wrote staging to {staging_path}")
        logger.info(f"[RUN_ID={run_id}] [STEP: READ] SUCCESS")

        return staging_path
    
    except Exception as e:
        logger.error(f"[RUN_ID={run_id}] [STEP: READ] FAILED: {str(e)}")
        raise


def step_save(data, run_id, logical_date):
    logger.info(f"[RUN_ID={run_id}] [STEP: SAVE] START")

    try:
        file_path = save_json(
            data=data,
            base_path=BASE_DATA_LAKE_PATH,
            entity=PRODUCTS,
            run_id=run_id,
            logical_date=logical_date
        )

        logger.info(f"[RUN_ID={run_id}] Saved to {file_path}")
        logger.info(f"[RUN_ID={run_id}] [STEP: SAVE] SUCCESS")

    except Exception as e:
        logger.error(f"[RUN_ID={run_id}] [STEP: SAVE] FAILED: {str(e)}")
        raise


def run_products_ingestion(run_id, logical_date):
    logger.info(f"========== RUN START | run_id={run_id} ==========")

    source_file = build_source_path(run_id)

    step_generate(source_file, run_id)
    data = step_read(source_file, run_id)
    step_save(data, run_id, logical_date)

    logger.info(f"========== RUN END | run_id={run_id} ==========")