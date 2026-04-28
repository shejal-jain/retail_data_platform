from retail_data_platform.config.config import BASE_DATA_LAKE_PATH, ORDERS
from retail_data_platform.ingestion.sources.order_source import generate_orders
from retail_data_platform.ingestion.loaders.file_loader import save_json
from retail_data_platform.ingestion.utils.logger import get_logger

logger = get_logger("ingest_orders")


def step_generate(run_id):
    logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] START")

    try:
        data = generate_orders(50)

        logger.info(f"[RUN_ID={run_id}] Generated {len(data)} records")
        logger.info(f"[RUN_ID={run_id}] [STEP: GENERATE] SUCCESS")

        return data

    except Exception as e:
        logger.error(f"[RUN_ID={run_id}] [STEP: GENERATE] FAILED: {str(e)}")
        raise
    

def step_save(data,run_id, logical_date):
    try:
        save_json(data, BASE_DATA_LAKE_PATH, ORDERS,run_id, logical_date)
    except Exception as e:
        logger.error(f"Save failed: {str(e)}")
        raise


def run_orders_ingestion(run_id, logical_date):
    logger.info(f"Orders ingestion started | run_id={run_id}")

    data = step_generate()
    step_save(data,run_id, logical_date)

    logger.info("Orders ingestion completed")