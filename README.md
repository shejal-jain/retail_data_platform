# Retail Data Platform (Airflow + Data Lake + PostgreSQL)

## 📌 Overview

This project implements a **production-style end-to-end data pipeline** using Apache Airflow.
It simulates a retail data platform that ingests, processes, validates, and stores data for analytics.

The pipeline follows a modern data architecture:

```text
Data Generation → Ingestion → Data Lake → Transformation → Data Quality → Parquet → PostgreSQL
```

---

## 🚀 Key Features

* Apache Airflow-based orchestration (DAG scheduling & task management)
* Modular ingestion pipelines for:

  * Customers
  * Products
  * Orders
* Step-based pipeline design (generate → read → save)
* Idempotent execution using `run_id`
* Partitioned data lake architecture (date-based)
* Transformation layer:

  * Join (customers + orders)
  * Aggregation (total orders, revenue)
  * Derived metrics (price × quantity)
* Data validation:

  * Skip invalid records (negative price, zero quantity)
  * Error handling and logging
* Data quality reporting:

  * Valid/invalid record counts
  * Percentage of bad data
* PostgreSQL integration for analytics querying
* Columnar storage using **Parquet**
* Partitioning using `date=YYYY-MM-DD` format (query optimization)
* XCom-based communication between Airflow tasks

---

## 🏗️ Architecture

```text
                +------------------+
                |   Airflow DAG    |
                +--------+---------+
                         |
        -------------------------------------
        |           |            |
   Customers     Products      Orders
   Pipeline      Pipeline      Pipeline
        \           |            /
         \          |           /
          ----------+-----------
                     |
             Transformation Layer
                     |
        ------------------------------
        |                            |
   Data Lake (Raw JSON)      Data Quality Reports
        |
   Data Lake (Parquet - Partitioned)
        |
     PostgreSQL (Analytics Layer)
```

---

## 📂 Project Structure

```text
airflow_project/
│
├── dags/
│   └── retail_pipeline_dag.py        # Airflow DAG
│
├── retail_data_platform/
│   ├── config/
│   │   └── config.py                # Paths & constants
│   │
│   ├── ingestion/
│   │   ├── sources/                # Data generation
│   │   ├── loaders/                # JSON, Parquet, DB loaders
│   │   ├── pipeline/               # Step-based ingestion logic
│   │   └── utils/                  # Logging utilities
│   │
│   ├── transformations/
│   │   └── customer_sales.py       # Transformation logic
│   │
│   ├── data_source/                # Generated CSV files
│   │
│   └── data_lake/
│       ├── raw/                    # JSON raw data
│       └── analytics/
│           ├── customer_sales/
│           │   └── date=YYYY-MM-DD/
│           └── data_quality/
│
├── docker-compose.yaml             # Airflow + Postgres setup
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup Instructions

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd airflow_project
```

---

### 2. Start Services

```bash
docker compose up -d
```

---

### 3. (If dependencies changed)

```bash
docker compose up --build -d
```

---

### 4. Access Airflow UI

```text
http://localhost:8080
```

* Enable DAG: `retail_data_pipeline`
* Trigger DAG manually

---

## 🗄️ PostgreSQL (Analytics Layer)

Separate database for pipeline output (not Airflow metadata DB)

* Host: `postgres_retail`
* Port: `5433`
* Database: `retail_db`
* User: `airflow`
* Password: `airflow`

### Connect to DB

```bash
docker exec -it retail_postgres psql -U airflow -d retail_db
```

---

## 📊 Example Queries

```sql
SELECT * FROM customer_sales LIMIT 10;

SELECT customer_id, total_amount
FROM customer_sales
ORDER BY total_amount DESC;
```

---

## 🔄 Pipeline Flow

1. Generate synthetic data (customers, products, orders)
2. Read and validate data
3. Store raw data in data lake (JSON)
4. Transform data:

   * Join customers and orders
   * Compute total_orders and total_amount
5. Apply data validation checks
6. Generate data quality report
7. Store analytics output in Parquet (partitioned)
8. Load results into PostgreSQL

---
