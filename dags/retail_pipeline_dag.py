import sys
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# -----------------------------
# Customers Imports
# -----------------------------
from retail_data_platform.ingestion.pipeline.ingest_customers import (
    step_generate as customers_generate,
    step_read as customers_read,
    step_save as customers_save,
    build_source_path as customers_source_path
)

# -----------------------------
# Products Imports
# -----------------------------
from retail_data_platform.ingestion.pipeline.ingest_products import (
    step_generate as products_generate,
    step_read as products_read,
    step_save as products_save,
    build_source_path as products_source_path
)

# -----------------------------
# Orders Imports
# -----------------------------
from retail_data_platform.ingestion.pipeline.ingest_orders import (
    step_generate as orders_generate,
    step_save as orders_save
)


default_args = {
    "owner": "retail_pipeline",
    "retries": 1,
}


with DAG(
    dag_id="retail_data_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # ============================================
    # CUSTOMERS TASKS
    # ============================================

    def customers_generate_task(**context):
        run_id = context["run_id"]

        source_file = customers_source_path(run_id)
        customers_generate(source_file, run_id)

        return source_file


    def customers_read_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]

        source_file = context["ti"].xcom_pull(task_ids="customers_generate")
        staging_path = customers_read(source_file, run_id, logical_date)

        return staging_path


    def customers_save_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]

        staging_path = context["ti"].xcom_pull(task_ids="customers_read")

        customers_save(staging_path, run_id, logical_date)


    customers_generate_op = PythonOperator(
        task_id="customers_generate",
        python_callable=customers_generate_task
    )

    customers_read_op = PythonOperator(
        task_id="customers_read",
        python_callable=customers_read_task
    )

    customers_save_op = PythonOperator(
        task_id="customers_save",
        python_callable=customers_save_task
    )


    # ============================================
    # PRODUCTS TASKS
    # ============================================

    def products_generate_task(**context):
        run_id = context["run_id"]

        source_file = products_source_path(run_id)
        products_generate(source_file, run_id)

        return source_file


    def products_read_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]

        source_file = context["ti"].xcom_pull(task_ids="products_generate")
        staging_path = products_read(source_file, run_id, logical_date)

        return staging_path


    def products_save_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]

        staging_path = context["ti"].xcom_pull(task_ids="products_read")

        products_save(staging_path, run_id, logical_date)


    products_generate_op = PythonOperator(
        task_id="products_generate",
        python_callable=products_generate_task
    )

    products_read_op = PythonOperator(
        task_id="products_read",
        python_callable=products_read_task
    )

    products_save_op = PythonOperator(
        task_id="products_save",
        python_callable=products_save_task
    )


    # ============================================
    # ORDERS TASKS
    # ============================================

    def orders_generate_task(**context):
        run_id = context["run_id"]

        data = orders_generate(run_id)
        return data


    def orders_save_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]

        data = context["ti"].xcom_pull(task_ids="orders_generate")

        orders_save(data, run_id, logical_date)


    orders_generate_op = PythonOperator(
        task_id="orders_generate",
        python_callable=orders_generate_task
    )

    orders_save_op = PythonOperator(
        task_id="orders_save",
        python_callable=orders_save_task
    )

     # ============================================
    # Transformation tasks
    # ============================================

    from retail_data_platform.transformations.customer_sales import run_customer_sales

    def customer_sales_task(**context):
        run_id = context["run_id"]
        logical_date = context["logical_date"]

        return run_customer_sales(run_id, logical_date)

    customer_sales_op = PythonOperator(
        task_id = "customer_sales_transformation",
        python_callable=customer_sales_task
    )

    #============================================
    #Inserting customer sales data to POSTGRES
    #=============================================

    from retail_data_platform.ingestion.loaders.db_loader import load_customer_sales_to_db

    def load_to_db_task(**context):
        file_path = context["ti"].xcom_pull(task_ids="customer_sales_transformation")
        load_customer_sales_to_db(file_path)

    load_to_db_op = PythonOperator(
        task_id = "load_customer_sales_to_db",
        python_callable = load_to_db_task
    )

    # ============================================
    # DEPENDENCIES
    # ============================================

    # Customers
    customers_generate_op >> customers_read_op >> customers_save_op

    # Products
    products_generate_op >> products_read_op >> products_save_op

    # Orders
    [customers_save_op, products_save_op] >> orders_generate_op
    orders_generate_op >> orders_save_op >> customer_sales_op

    #Data insertion into Postgres
    customer_sales_op >> load_to_db_op