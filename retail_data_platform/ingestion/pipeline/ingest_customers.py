import os
import json

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


def step_generate(source_file, run_id):
    logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] START")

    try:
        if os.path.exists(source_file):
            logger.info(f"[RUN_ID={run_id}] File exists, skipping: {source_file}")
            logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] SKIPPED")
            return

        generate_customers_csv(source_file, n=20)

        logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] SUCCESS")

    except Exception as e:
        logger.error(f"[RUN_ID={run_id}] [STEP: GENERATE] FAILED: {str(e)}")
        raise


def step_read(source_file, run_id, logical_date):
    logger.info(f"[RUN_ID={run_id}] [STEP: READ] START")

    try:
        if not os.path.exists(source_file):
            raise FileNotFoundError(f"{source_file} not found")

        data = read_csv(source_file)

          #write intermediate (staging file)
        staging_path=save_json(
            data=data,
            base_path=BASE_DATA_LAKE_PATH,
            entity=f"{CUSTOMERS}_staging",
            run_id=run_id,
            logical_date=logical_date
        )

        logger.info(f"[RUN_ID={run_id}] Read {len(data)} records")
        logger.info(f"[RUN_ID={run_id}] Wrote staging to {staging_path}")
        logger.info(f"[RUN_ID={run_id}] [STEP: READ] SUCCESS")

        return staging_path

    except Exception as e:
        logger.error(f"[RUN_ID={run_id}] [STEP: READ] FAILED: {str(e)}")
        raise


def step_save(staging_path, run_id, logical_date):
    logger.info(f"[RUN_ID={run_id}] [STEP: SAVE] START")

    try:
        # read from staging file
        with open(staging_path, "r") as f:
            data = json.load(f)

        final_path = save_json(
            data=data,
            base_path=BASE_DATA_LAKE_PATH,
            entity=CUSTOMERS,
            run_id=run_id,
            logical_date=logical_date
        )

        logger.info(f"[RUN_ID={run_id}] Saved to {final_path}")
        logger.info(f"[RUN_ID={run_id}] [STEP: SAVE] SUCCESS")

    except Exception as e:
        logger.error(f"[RUN_ID={run_id}] [STEP: SAVE] FAILED: {str(e)}")
        raise


def run_customers_ingestion(run_id, logical_date):
    logger.info(f"========== RUN START | run_id={run_id} ==========")

    source_file = build_source_path(run_id)

    step_generate(source_file, run_id)
    data = step_read(source_file, run_id)
    step_save(data, run_id, logical_date)

    logger.info(f"========== RUN END | run_id={run_id} ==========")

    logger.info("Customers ingestion completed")