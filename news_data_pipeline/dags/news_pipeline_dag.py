import sys
sys.path.insert(0, "/opt/airflow")
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv

from news_data_pipeline.ingestion.news_api_client import fetch_news
from news_data_pipeline.storage.save_raw import save_raw
from news_data_pipeline.transformation.clean_articles import clean_articles_data
from news_data_pipeline.storage.save_processed import save_processed
from news_data_pipeline.db.load_postgres import load_postgres
from pathlib import Path
import json


load_dotenv()

def fetch_and_save_raw():
    api_key = os.getenv("NEWS_API_KEY")
    data = fetch_news(api_key)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = save_raw(data,timestamp)
    print("Fetched and saved raw data")
    return str(file_path)

def clean_and_save_processed(ti):
    # Get file path from Task 1 (XCom)
    raw_file_path = ti.xcom_pull(task_ids="fetch_and_save_raw")
    print(f"Reading raw file: {raw_file_path}")

    with open(raw_file_path, "r") as f:
        data = json.load(f)

    articles = data["articles"]

    clean_articles = clean_articles_data(articles)

    #Extract timestamp from file
    filename = Path(raw_file_path).stem
    timestamp = filename.split("_",1)[1]

    processed_file_path = save_processed(clean_articles, timestamp)

    print("Processed data saved.")
    return str(processed_file_path)

def load_to_postgres(ti):
    processed_dir = Path('/opt/airflow/news_data_pipeline/data/processed')
    
    #Get latest processed file
    # Get file path from Task 1 (XCom)
    processed_file_path = ti.xcom_pull(task_ids="clean_and_save_processed")
    print(f"Reading processed file: {processed_file_path}")

    with open(processed_file_path,"r") as f:
        clean_articles = json.load(f)
    
    load_postgres(clean_articles)

    print("Data loaded to PostgreSQL")

with DAG(
    dag_id="news_pipeline_dag",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
)as dag:
    
    fetch_raw = PythonOperator(
        task_id="fetch_and_save_raw",
        python_callable=fetch_and_save_raw
    )
  
    clean_task = PythonOperator(
        task_id="clean_and_save_processed",
        python_callable=clean_and_save_processed
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    fetch_raw >> clean_task >> load_task