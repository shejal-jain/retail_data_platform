import uuid
from datetime import datetime, timezone
from retail_data_platform.ingestion.pipeline.ingest_customers import run_customers_ingestion
from retail_data_platform.ingestion.pipeline.ingest_products import run_products_ingestion
from retail_data_platform.ingestion.pipeline.ingest_orders import run_orders_ingestion

def run_all():
    run_id = str(uuid.uuid4())
    logical_date = datetime.now(timezone.utc)

    print(f"Starting pipelines | run_id={run_id}")

    run_customers_ingestion(run_id, logical_date)
    run_products_ingestion(run_id, logical_date)
    run_orders_ingestion(run_id, logical_date)

