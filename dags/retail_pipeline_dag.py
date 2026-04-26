import sys
sys.path.insert(0,"/opt/airflow")
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

# Import your existing pipeline functions
from retail_data_platform.ingestion.pipeline.ingest_customers import run_customers_ingestion
from retail_data_platform.ingestion.pipeline.ingest_products import run_products_ingestion
from retail_data_platform.ingestion.pipeline.ingest_orders import run_orders_ingestion


# Default arguments for DAG
default_args = {
    "owner": "retail_pipeline",
    "retries": 1,  # retry once if task fails
}


# Define DAG
with DAG(
    dag_id="retail_data_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # -----------------------------
    # Customers Task
    # -----------------------------
    def customers_task(**context):
        """
        Wrapper function:
        - Gets run_id from Airflow
        - Calls your ingestion function
        """
        run_id = context["run_id"]
        logical_date = context["logical_date"]
        run_customers_ingestion(run_id, logical_date)


    customers = PythonOperator(
        task_id="customers_ingestion",
        python_callable=customers_task
    )


    # -----------------------------
    # Products Task
    # -----------------------------
    def products_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]
        run_products_ingestion(run_id, logical_date)


    products = PythonOperator(
        task_id="products_ingestion",
        python_callable=products_task
    )


    # -----------------------------
    # Orders Task
    # -----------------------------
    def orders_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]
        run_orders_ingestion(run_id, logical_date)


    orders = PythonOperator(
        task_id="orders_ingestion",
        python_callable=orders_task
    )


    # -----------------------------
    # Task Dependencies
    # -----------------------------

    # Customers and Products run in parallel
    # Orders runs after both complete
    [customers, products] >> orders