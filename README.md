# Production-Grade Cloud-Native Data Platform & Automated Governance Framework

An end-to-end, event-driven data platform built natively on Google Cloud Platform (GCP). The system orchestrates containerized microservices, decoupled serverless transformation workloads, and an automated data governance framework under a zero-polling, fully reactive architecture.

Originally conceived as a localized data pipeline utilizing FileSensors and local dbt configurations, this project has evolved into an enterprise-ready blueprint leveraging **Cloud Composer 3**, **Cloud Run Jobs**, and **Google Cloud Dataplex**.

---

## 🏗️ Architecture Overview

```
[ User Upload ] ──> FastAPI (Cloud Run Service) ──> Google Cloud Storage (GCS)
│
(Eventarc Trigger)
▼
BigQuery Raw <── [ Ingestion & Quarantine ] 🧱 ── Cloud Run Function
│
(REST API Trigger)
▼
Cloud Composer 3 (Orchestration Engine)
│
├──🔀 Cloud Run Job 1: dbt-staging-job  (Raw Validation & Quarantine Split)
├──🔀 Cloud Run Job 2: dbt-transform-job (Warehouse Core & Star Schema)
└──🔀 Cloud Run Job 3: dbt-marts-test-job (Data Mart Testing)
│
└───► [ Trigger DagRun Operator ] ──► Dataplex Governance Pipeline
├── 📊 Profile Scans
└── 📐 Quality Scans (MTD/QTD/YTD/Yearly)
```

The platform operates on a completely serverless, decoupled execution model where **Cloud Composer 3** serves as the central orchestration engine.
### End-to-End Data Lifecycle Flow
1. **Event-Driven Ingestion:** A public-facing containerized **FastAPI service** running on Cloud Run handles source file uploads, streaming them straight into an input Google Cloud Storage (GCS) bucket.
2. **Serverless Processing:** The moment a file lands, **Eventarc** intercepts the storage mutation and routes it directly to an ingestion **Cloud Run Function**. This function parses, validates, and loads the data into the BigQuery Raw dataset.
3. **Orchestrated Activation:** Upon successful database load, the function hits the Cloud Composer 3 REST API endpoint to trigger the main orchestration pipeline (`transform.py`).
4. **Decoupled Transformation:** Cloud Composer executes three isolated **Cloud Run Jobs** sequentially via the `CloudRunExecuteJobOperator`. This guarantees a rigid operational order: **raw data validation/quarantine → warehouse transformation → mart validation**.
5. **Automated Governance:** Once transformations clear, the main pipeline downstreams into a secondary dedicated governance DAG (`data_quality_profile.py`) that executes complex, parallel **Google Cloud Dataplex Data Quality and Profiling scans**.

---

## ⚡ Key Architectural Features

### 🛡️ BigQuery Quarantine Layer
To protect downstream data marts from contamination, an isolated **Quarantine Dataset layer** sits at the gate of the Medallion architecture. Schema anomalies, data type mismatches, and structural violations are intercepted during ingestion and staging, then routed directly into quarantine tables with associated metadata for subsequent analysis. This guarantees a pristine staging environment.

### 🧩 Decoupled dbt Execution via Cloud Build
Instead of installing dbt natively within the Cloud Composer environment—which bloats the worker image and wastes compute overhead—transformations are modularized into stateless containers. Every code change pushes through a **Cloud Build CI/CD pipeline**, building separate target images and registering them as executable **Cloud Run Jobs**.

### 📉 Non-Blocking Asynchronous Operators
To maximize resource utilization and reduce Cloud Composer footprint costs, long-running processes (Cloud Run Jobs and Dataplex Scans) are configured using **Airflow Deferrable Operators (`deferrable=True`)**. Tasks immediately yield their worker slots to the asynchronous Triggerer layer while waiting for external cloud callbacks, completely eliminating worker thread saturation.

---

## 🚀 Tech Stack

* **Orchestration:** Cloud Composer 3 (Apache Airflow / TaskFlow API), Astro CLI
* **Data Governance & Quality:** Google Cloud Dataplex
* **Data Warehousing:** Google BigQuery (SQL, dbt Core)
* **Compute & Serverless:** Cloud Run (Services & Jobs), Cloud Run Functions
* **CI/CD & Dev Tools:** Cloud Build, Docker, Git
* **Ingestion API:** FastAPI, Uvicorn, Python
* **Event Routing:** Eventarc, Google Cloud Storage (GCS)