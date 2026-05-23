# Cloud Native Data Platform & Automated Governance Framework

A fully serverless, event-driven, cloud native data platform on GCP that ingests monthly Excel sales data through a web UI, loads it into BigQuery, orchestrates dbt transformations via Cloud Composer, creates a warehouse and governs data quality using Knowledge Catalog (Dataplex).

---

## Architecture

```
Browser (FastAPI Upload UI)
        │  drag-and-drop .xlsx
        ▼
Cloud Run — FastAPI service
        │  stream file to GCS
        ▼
Google Cloud Storage (sales-dw-bucket)
        │  OBJECT_FINALIZE → Eventarc
        ▼
Cloud Run — extract-load function
        │  read xlsx → clean → quarantine bad rows → load BigQuery raw_data.sales
        │  POST /api/v1/dags/transformation_pipeline/dagRuns
        ▼
Cloud Composer 3 (Airflow 2.x)
        │  transformation_pipeline DAG  (schedule=None)
        │
        ├── dbt-staging-job (Cloud Run Job)
        │     source freshness → source tests → quarantine → staging → staging tests
        │
        ├── dbt-transform-job (Cloud Run Job)
        │     dbt run --select marts
        │
        ├── dbt-marts-test-job (Cloud Run Job)
        │     dbt test --select marts
        │
        ├── send_success_email / send_failure_email
        │
        └── TriggerDagRunOperator
                ▼
        automated_data_quality_check_and_profile_scan_pipeline DAG
                │
                ├── Quality scans — fct, revenue, growth, rankings, shipment
                ├── Profile scan  — fct_grocery_sales column statistics
                └── send_summary_email → warehouse.dq_results
```

---

## Project Evolution

This project started as a local Airflow + dbt pipeline — a `FileSensor` waiting for an Excel file on disk, a Python operator loading it into BigQuery, and a `DbtTaskGroup` building the warehouse. It worked but polled, waited, and was tied to a machine.

The first cloud iteration introduced Eventarc, Cloud Run, and GCS as the ingestion layer while keeping Airflow and dbt running locally, exposed via ngrok. The final iteration — this repository — eliminated every local component:

| Component | Before | After |
|-----------|--------|-------|
| Upload UI | `uvicorn main:app` locally | Cloud Run service |
| Airflow | Astro CLI + Docker locally | Cloud Composer 3 |
| dbt | DbtTaskGroup inside Airflow | 3 Cloud Run Jobs |
| Trigger | ngrok tunnel | Composer REST API |
| Data quality | dbt tests only | dbt tests + Dataplex Universal Catalog |
| Bad data | Silently filtered | Quarantined to BigQuery `quarantine` dataset |

---

## Repository Structure

```
cloud-native-data-platform/
│
├── cloud-run-fastapi-server/                        # FastAPI upload UI (Cloud Run service)
│   ├── main.py                        # /upload endpoint → GCS
│   ├── index.html                     # drag-and-drop upload interface
│   ├── Dockerfile
│   └── requirements.txt
│
├── cloud_run-function/                         # Extract-load function (Cloud Run service)
│   ├── main.py                        # GCS → clean → BQ → trigger Composer
│   └── requirements.txt
│
├── cloud-run-dbt-jobs/                          # dbt Cloud Run Jobs
│   ├── Dockerfile.staging             # Job 1: source tests + quarantine + staging
│   ├── Dockerfile.transform           # Job 2: dbt run --select marts
│   ├── Dockerfile.marts-test          # Job 3: dbt test --select marts
│   ├── cloudbuild.staging.yml
│   ├── cloudbuild.transform.yml
│   ├── cloudbuild.marts-test.yml
│   ├── profiles.yml                   # BigQuery connection (method: oauth, ADC)
│   ├── requirements.txt               # dbt-core + dbt-bigquery
│   ├── dbt_project.yml
│   ├── packages.yml                   # dbt_utils
│   ├── models/
│   │   ├── sources.yml                # raw_data.sales source + freshness check
│   │   ├── staging/
│   │   │   ├── stg_sales.sql          # single staging model with quarantine filter
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── dimension/             # 7 dimension tables
│   │       ├── fact/                  # fct_grocery_sales
│   │       ├── rankings/              # 5 ranking models
│   │       ├── ytd_mtd_qtd_growth/    # YTD, MTD, QTD growth models
│   │       ├── monthly_revenue_growth.sql
│   │       ├── quarterly_revenue_growth.sql
│   │       ├── yearly_revenue_growth.sql
│   │       ├── revenue_qty_sales_trend_by_year_quarter_month.sql
│   │       ├── shipment_exp_comparison_by_ship_company.sql
│   │       └── schema.yml             # generic + singular tests for all mart models
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── quarantine_bad_rows.sql    # writes bad rows to quarantine dataset
│   └── tests/                         # 6 custom singular tests
│
├── composer/                          # Cloud Composer 3 DAGs and setup
│   ├── dags/
│   │   ├── transform.py               # main ELT orchestration DAG (TaskFlow API)
│   │   └── data_quality_profile.py    # Dataplex quality + profile scan DAG
│   ├── setup_composer.sh              # create environment + grant permissions
│   ├── deploy_dags.sh                 # upload DAGs to Composer GCS bucket
│   ├── setup_connections.sh           # configure SMTP connection
│   └── composer_requirements.txt
│
└── docs/
    ├── 01_fastapi_cloud_run.md
    ├── 02_dbt_cloud_run_jobs.md
    ├── 02b_cloud_run_extract_load_function.md
    ├── 03_cloud_composer_orchestration.md
    ├── 04_dataplex_universal_catalog.md
    └── schema_diagram.png             # star schema diagram
```

---

## Data Warehouse Schema

The warehouse is built on a **star schema** in BigQuery with three layers:

```
raw_data.sales              ← source table, append-only, monthly batch
quarantine.stg_sales_rejected_YYYYMMDD  ← bad rows captured before staging

staging.stg_sales           ← cleaned view, good rows only

warehouse.dim_customer      ┐
warehouse.dim_product       │
warehouse.dim_salesperson   │  7 dimension tables
warehouse.dim_shipper       │  surrogate keys via dbt_utils.generate_surrogate_key
warehouse.dim_region        │
warehouse.dim_payment_type  │
warehouse.dim_date          ┘  date spine 2020–2026, fiscal + calendar attributes

warehouse.fct_grocery_sales ← central fact table, grain: order_id + product_name
                               incremental materialisation, unique_key: sale_key
                               joined to dim_date twice (order_date + shipped_date)

warehouse.monthly_revenue_growth
warehouse.quarterly_revenue_growth
warehouse.yearly_revenue_growth
warehouse.revenue_qty_sales_trend_by_year_quarter_month
warehouse.shipment_exp_comparison_by_ship_company
warehouse.city_rank_by_revenue_qty_sales
warehouse.customer_rank_by_revenue_qty_sales
warehouse.product_rank_by_revenue_qty_sales
warehouse.region_rank_by_revenue_qty_sales
warehouse.salesperson_rank_by_revenue_qty_sales
warehouse.ytd_revenue_qty_shipment_growth
warehouse.mtd_revenue_qty_shipment_growth
warehouse.qtd_revenue_qty_shipment_growth

warehouse.dq_results        ← Dataplex scan results, auto-managed by GCP
```

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Upload UI | FastAPI, HTML/CSS/JS |
| File storage | Google Cloud Storage |
| Event trigger | Eventarc (Cloud Storage trigger, OBJECT_FINALIZE) |
| Extract-Load | Cloud Run service, Python, pandas, BigQuery client |
| Orchestration | Cloud Composer 3, Airflow 2.x, TaskFlow API |
| Transformation | dbt Core 1.8, dbt-bigquery |
| Job execution | Cloud Run Jobs, Cloud Build |
| Data warehouse | BigQuery |
| Data governance | Dataplex Universal Catalog |
| Alerting | Gmail SMTP, Airflow EmailOperator |
| Auth | Google ADC, AuthorizedSession, IAM |
| IaC / Deploy | gcloud CLI, Cloud Build |

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| Google Cloud SDK (`gcloud`) | Deploy Cloud Run, Composer, Cloud Build |
| GCP project with billing enabled | All GCP services |
| Python 3.12+ | Local development |
| Docker Desktop | Build and test containers locally |

---

## GCP Services Required

Enable these APIs in your project:

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  composer.googleapis.com \
  dataplex.googleapis.com \
  datalineage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project=YOUR_PROJECT_ID
```

---

## Setup — Step by Step

### 1. GCS bucket

```bash
gcloud storage buckets create gs://sales-dw-bucket --location=US
```

### 2. BigQuery datasets

```bash
bq mk --dataset YOUR_PROJECT:raw_data
bq mk --dataset YOUR_PROJECT:staging
bq mk --dataset YOUR_PROJECT:warehouse
bq mk --dataset YOUR_PROJECT:quarantine
```

> Do **not** create `warehouse.dq_results` manually — Dataplex creates and
> manages this table automatically on first scan run.

### 3. Service accounts

**FastAPI + extract-load SA:**

```bash
gcloud iam service-accounts create bigquery-service \
  --display-name="Stratum Upload and Extract-Load SA"

for role in \
  roles/storage.objectCreator \
  roles/storage.objectViewer \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/composer.user; do
  gcloud projects add-iam-policy-binding YOUR_PROJECT \
    --member="serviceAccount:bigquery-service@YOUR_PROJECT.iam.gserviceaccount.com" \
    --role="$role"
done
```

**Dataplex managed SA (auto-created when Dataplex API is enabled):**

```bash
PROJECT_NUMBER=$(gcloud projects describe YOUR_PROJECT --format="value(projectNumber)")
DATAPLEX_SA="service-${PROJECT_NUMBER}@gcp-sa-dataplex.iam.gserviceaccount.com"

for role in \
  roles/bigquery.dataViewer \
  roles/bigquery.jobUser \
  roles/bigquery.dataEditor; do
  gcloud projects add-iam-policy-binding YOUR_PROJECT \
    --member="serviceAccount:${DATAPLEX_SA}" \
    --role="$role"
done
```

### 4. Deploy FastAPI upload server

```bash
cd web_portal

gcloud run deploy fastapi-upload \
  --source . \
  --region asia-south2 \
  --allow-unauthenticated \
  --service-account=bigquery-service@YOUR_PROJECT.iam.gserviceaccount.com \
  --set-env-vars GCS_BUCKET_NAME=sales-dw-bucket
```

### 5. Deploy Cloud Run extract-load function

Wire the Eventarc trigger at deploy time:

```bash
cd cloud_run

gcloud run deploy extract-load \
  --source . \
  --region asia-south2 \
  --no-allow-unauthenticated \
  --service-account=bigquery-service@YOUR_PROJECT.iam.gserviceaccount.com \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=sales-dw-bucket" \
  --memory=1Gi
```

### 6. Deploy dbt Cloud Run Jobs

```bash
cd dbt-jobs

# Create the Dataplex SA permissions first (Step 3 above)
# Then deploy all three jobs
gcloud builds submit --config cloudbuild.staging.yml .
gcloud builds submit --config cloudbuild.transform.yml .
gcloud builds submit --config cloudbuild.marts-test.yml .
```

### 7. Create Cloud Composer 3 environment

```bash
gcloud composer environments create stratum-composer \
  --location=asia-south2 \
  --composer-version=3 \
  --project=YOUR_PROJECT
```

Creation takes 20–30 minutes. Get the webserver URL after:

```bash
gcloud composer environments describe stratum-composer \
  --location=asia-south2 \
  --format="value(config.airflowUri)"
```

Update `WEB_SERVER_URL` in `cloud_run/main.py` with this URL then redeploy
the extract-load function.

### 8. Configure Composer environment

```bash
# Set Airflow config overrides
gcloud composer environments update stratum-composer \
  --location=asia-south2 \
  --update-airflow-configs="\
core-dags_are_paused_at_creation=True,\
core-max_active_runs_per_dag=1,\
scheduler-min_file_process_interval=60"

# Set environment variables
gcloud composer environments update stratum-composer \
  --location=asia-south2 \
  --update-env-variables="\
PROJECT_ID=YOUR_PROJECT,\
REGION=asia-south2,\
ALERT_EMAIL=your-email@gmail.com"

# Set SMTP email config overrides in GCP Console:
# email/email_backend = airflow.utils.email.send_email_smtp
# smtp/smtp_host = smtp.gmail.com
# smtp/smtp_starttls = True
# smtp/smtp_ssl = False
# smtp/smtp_port = 587
# smtp/smtp_mail_from = your-email@gmail.com

# Add SMTP connection
gcloud composer environments run stratum-composer \
  --location=asia-south2 \
  connections add smtp_default \
  -- \
  --conn-type=smtp \
  --conn-host=smtp.gmail.com \
  --conn-port=587 \
  --conn-login=your-email@gmail.com \
  --conn-password=YOUR_GMAIL_APP_PASSWORD
```

### 9. Grant Composer SA permissions

```bash
COMPOSER_SA=$(gcloud composer environments describe stratum-composer \
  --location=asia-south2 \
  --format="value(config.nodeConfig.serviceAccount)")

for role in \
  roles/composer.worker \
  roles/run.invoker \
  roles/run.viewer \
  roles/dataplex.editor \
  roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding YOUR_PROJECT \
    --member="serviceAccount:${COMPOSER_SA}" \
    --role="$role"
done

# SA-level binding so Composer can act as the Cloud Run Job SA
gcloud iam service-accounts add-iam-policy-binding \
  bigquery-service@YOUR_PROJECT.iam.gserviceaccount.com \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/iam.serviceAccountUser"
```

### 10. Upload DAGs

```bash
BUCKET=$(gcloud composer environments describe stratum-composer \
  --location=asia-south2 \
  --format="value(config.dagGcsPrefix)")

gcloud storage cp composer/dags/transform.py             $BUCKET/
gcloud storage cp composer/dags/data_quality_profile.py  $BUCKET/
```

Unpause both DAGs in the Airflow UI or via CLI:

```bash
gcloud composer environments run stratum-composer \
  --location=asia-south2 \
  dags unpause -- transformation_pipeline

gcloud composer environments run stratum-composer \
  --location=asia-south2 \
  dags unpause -- automated_data_quality_check_and_profile_scan_pipeline
```

---

## Running the Pipeline

Upload a monthly Excel sales file through the FastAPI UI:

```
https://fastapi-upload-xxxx-as.a.run.app
```

**What happens automatically:**

```
1. FastAPI streams the file to GCS
2. Eventarc fires OBJECT_FINALIZE → Cloud Run extract-load function
3. Function reads xlsx, cleans columns, deduplicates by batch_id
4. Bad rows written to quarantine.stg_sales_rejected_YYYYMMDD
5. Clean data loaded to raw_data.sales in BigQuery
6. Composer REST API called → transformation_pipeline DAG triggered
7. dbt-staging-job: freshness check → source tests → quarantine macro → staging run → staging tests
8. dbt-transform-job: builds all 7 dims, fact table, 13 mart models
9. dbt-marts-test-job: 124 tests across all mart models
10. Success email sent to configured address
11. Dataplex quality DAG triggered:
    - Quality scans on fct, revenue models, growth models, rankings, shipment
    - Profile scan on fct_grocery_sales
    - Results exported to warehouse.dq_results
    - Summary email sent
```

### Manual trigger for testing

```bash
gcloud composer environments run stratum-composer \
  --location=asia-south2 \
  dags trigger -- transformation_pipeline
```

---

## Data Quality Framework

Two complementary layers:

**Layer 1 — dbt tests (pipeline gate):**
Run inside Cloud Run Jobs during transformation. Failing tests stop the pipeline
and trigger an alert email. Covers structural integrity, referential integrity,
financial validity, and 6 custom singular tests.

**Layer 2 — Dataplex Universal Catalog (governance layer):**
Run after transformation completes. Results are stored in `warehouse.dq_results`
and visible in the GCP Console. Covers all mart models with completeness,
uniqueness, and validity rules. Profile scan on `fct_grocery_sales` detects
data drift across monthly loads.

### Quarantine

Bad rows from `raw_data.sales` are written to `quarantine.stg_sales_rejected_YYYYMMDD`
before staging. Each row includes `quarantined_at` and `failed_condition` columns
identifying exactly which rule caused rejection.

```sql
-- Review quarantined rows from latest batch
SELECT failed_condition, COUNT(*) AS row_count
FROM `YOUR_PROJECT.quarantine.stg_sales_rejected_YYYYMMDD`
GROUP BY failed_condition
ORDER BY row_count DESC
```

---

## Key Design Decisions

**`schedule=None` on both DAGs** — neither DAG runs on a schedule. The ELT
pipeline wakes up only when a file lands in GCS. The quality DAG wakes up only
when the ELT pipeline completes. The system reacts to data, not time.

**One Docker image per dbt job** — three separate Dockerfiles with different
`CMD` instructions. Separate images make each job independently deployable and
give clear failure boundaries: Job 1 fail = bad source data, Job 2 fail =
transformation logic error, Job 3 fail = mart model regression.

**`method: oauth` in dbt profiles.yml** — uses Application Default Credentials
via the Cloud Run Job's attached service account. No JSON key files inside
containers.

**`deferrable=True` on Cloud Run and Dataplex operators** — workers submit
jobs and immediately free their slots. The Airflow Triggerer handles async
polling via `asyncio`. Prevents worker exhaustion on long-running jobs.

**`NONE_FAILED_MIN_ONE_SUCCESS` on join operators** — correctly propagates
skips through task group boundaries when upstream jobs are skipped, preventing
deferrable operators from being handed to the Triggerer unnecessarily.

**Dataplex `dq_results` table is never pre-created** — Dataplex owns the
schema of its export table. Pre-creating it with any schema causes a rejection
error. The table is created automatically on first successful scan run.

---

## Documentation

Each part of the project has a detailed standalone document covering setup,
design decisions, errors encountered, and fixes applied:

| Document | Covers |
|----------|--------|
| `docs/01_fastapi_cloud_run.md` | FastAPI containerisation and Cloud Run deployment |
| `docs/02_dbt_cloud_run_jobs.md` | dbt Cloud Run Jobs — Dockerfiles, Cloud Build, quarantine macro |
| `docs/02b_cloud_run_extract_load_function.md` | Extract-load function — GCS → BigQuery → Composer trigger |
| `docs/03_cloud_composer_orchestration.md` | Cloud Composer 3 setup, DAG design, trigger rule debugging |
| `docs/04_dataplex_universal_catalog.md` | Dataplex scans, deferrable operators, zombie task debugging |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Extract-load function 403 on Composer | SA missing `roles/composer.user` | Add role to bigquery-service SA |
| dbt job fails with `oauth` error | Wrong auth method in profiles.yml | Use `method: oauth` not `method: service-account` |
| Dataplex tasks hang 9+ minutes, no logs | Blocking poll causes zombie tasks | Add `deferrable=True` to Run operators |
| Tasks fail in queued state | Worker pod eviction from parallelism | Add `max_active_tasks=6`, sequential dependencies |
| `dq_results` schema mismatch | Table pre-created manually | Delete table, let Dataplex create it |
| Duplicate rows in fact table | Mixed manual + DAG-triggered runs | Full refresh: `dbt run --select fct_grocery_sales --full-refresh` |
| Wrong email sent after failure | Trigger rule propagation through task groups | Use `NONE_FAILED_MIN_ONE_SUCCESS` on join operators |
| DAG broken — `_TaskDecorator has no attribute roots` | `@task` function not invoked with `()` | Add `()` to all task/task_group calls |