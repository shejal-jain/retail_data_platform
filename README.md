# News Data Pipeline 

> A batch news ingestion and processing pipeline built to enable downstream analysis of news trends, sentiment, and source diversity — triggered on-demand or on a schedule, orchestrated end-to-end with Apache Airflow.

---

## Objective

Build a modular and reproducible data pipeline that:

* Ingests news data from an external API
* Processes and cleans raw data
* Stores both raw and processed datasets
* Loads structured data into a relational database
* Automates the workflow using Airflow

---

## Evolution of the Project

### Phase 1 — Manual Pipeline Execution

The pipeline was initially implemented using standalone Python scripts.

Flow:

1. Fetch news data from API
2. Save raw JSON data
3. Clean and transform articles
4. Save processed data
5. Load into PostgreSQL

Execution:

```bash
python news_data_pipeline/scripts/ingest_news_api.py
```

---

### Phase 2 — Airflow Orchestration

The pipeline was refactored and integrated with **Apache Airflow** for orchestration.

#### DAG Tasks:

1. `fetch_and_save_raw`
2. `clean_and_save_processed`
3. `load_to_postgres`

#### Improvements:

* Task-based execution
* Dependency management
* Scheduling capability
* Improved reliability

---

## Project Structure

```text
airflow_project/
│
├── docker-compose.yml
├── README.md
├── requirements.txt
├── .env.example
│
├── news_data_pipeline/
│   ├── dags/
│   │   └── news_pipeline_dag.py
│   ├── ingestion/
│   ├── transformation/
│   ├── storage/
│   ├── db/
│   ├── scripts/
│   └── __init__.py
```

---

## Tech Stack

| Layer            | Tool           |
| ---------------- | -------------- |
| Language         | Python         |
| Orchestration    | Apache Airflow |
| Containerisation | Docker         |
| Database         | PostgreSQL     |
| API Source       | NewsAPI        |
| DB Connector     | psycopg2       |

---

## Pipeline Flow

```text
NewsAPI
  └── Ingestion (fetch_and_save_raw)
        └── Transformation (clean_and_save_processed)
              └── Storage (raw JSON + processed JSON)
                    └── Load (load_to_postgres → PostgreSQL)
```

---

## Engineering Concepts Applied

* Modular pipeline design (ingestion, transformation, storage, db)
* Airflow DAG orchestration and task dependencies
* Docker-based environment setup
* Volume mounting and container file access
* Inter-container communication (Airflow ↔ PostgreSQL)
* Environment-based configuration using `.env`
* Deterministic pipelines using XCom for task communication

---

## Running the Project

### 1. Clone the repository

```bash
git clone <repo-url>
cd airflow_project
```

---

### 2. Configure environment variables

```bash
cp .env.example .env
cp news_data_pipeline/.env.example news_data_pipeline/.env
```

Update values as required.

> Do not commit `.env` files — they contain environment-specific configuration.

---

### 3. Start Airflow

```bash
docker-compose up --build
```

---

### 4. Access Airflow UI

```
http://localhost:8080
```

Default credentials (for local development only):

* Username: admin
* Password: admin

---

## Outcomes

* Built an end-to-end batch data pipeline
* Automated workflow using Airflow DAGs
* Implemented modular and maintainable pipeline design
* Containerised the system for reproducibility

---
