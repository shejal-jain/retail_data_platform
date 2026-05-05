# Retail Data Pipeline (Airflow + Data Lake + PostgreSQL)

## 📌 Overview

This project implements a **production-style end-to-end data pipeline** using Apache Airflow.
It simulates a retail data platform that ingests, processes, validates, and stores data for analytics.

The pipeline follows a layered architecture:

```
Data Generation → Ingestion → Transformation → Data Lake → Database → Analytics
```

---

## 🚀 Features

* End-to-end pipeline orchestration using **Apache Airflow**
* Modular ingestion pipelines (customers, products, orders)
* **Idempotent processing** using `run_id`
* Partitioned data lake structure (date-based)
* Transformation layer with:

  * Join logic (customers + orders)
  * Aggregations (total orders, total revenue)
  * Derived metrics (price × quantity)
* **Data validation checks**

  * Invalid records skipped (negative price, zero quantity)
  * Logging of anomalies
* **Data quality reporting**

  * Valid vs invalid records
  * Invalid percentage tracking
* PostgreSQL integration for analytics querying
* XCom-based task communication (file path passing)

---

## 🏗️ Architecture

```
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
          +----------+----------+
          |                     |
     Data Lake (JSON)     Data Quality Reports
          |
     PostgreSQL (Analytics Layer)
```

---

## 📂 Project Structure

```
airflow_project/
│
├── dags/
│   └── retail_pipeline_dag.py
│
├── retail_data_platform/
│   ├── config/
│   │   └── config.py
│   │
│   ├── ingestion/
│   │   ├── sources/        # Data generation
│   │   ├── loaders/        # File + DB loaders
│   │   ├── pipeline/       # Step-based ingestion logic
│   │   └── utils/          # Logging utilities
│   │
│   ├── transformations/
│   │   └── customer_sales.py
│   │
│   ├── data_source/        # Generated CSV files
│   └── data_lake/
│       ├── raw/
│       └── analytics/
│
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup Instructions

### 1. Clone Repository

```
git clone <your-repo-url>
cd airflow_project
```

---

### 2. Start Services

```
docker compose up -d
```

---

### 3. Access Airflow UI

```
http://localhost:8080
```

---

### 4. Trigger DAG

* Enable `retail_data_pipeline`
* Click "Trigger DAG"

---

## 🗄️ PostgreSQL Setup

The project uses a separate PostgreSQL container for analytics:

* Host: `postgres_retail`
* Port: `5433`
* Database: `retail_db`
* User: `airflow`
* Password: `airflow`

### Connect to DB

```
docker exec -it retail_postgres psql -U airflow -d retail_db
```

---

## 📊 Example Query

```
SELECT customer_id, total_amount
FROM customer_sales
ORDER BY total_amount DESC;
```

---

## 🔄 Pipeline Flow

1. Generate data (CSV simulation)
2. Read and validate data
3. Save raw data to data lake
4. Perform transformation:

   * Join customers and orders
   * Calculate metrics
5. Save analytics output
6. Generate data quality report
7. Load results into PostgreSQL

---

## 🧠 Key Concepts Demonstrated

* Airflow DAG orchestration
* Idempotent pipeline design
* Partitioned data lake architecture
* Data transformation (JOIN + GROUP BY)
* Data validation and quality checks
* XCom-based task communication
* Database integration (PostgreSQL)
* Separation of concerns in pipeline design

---

