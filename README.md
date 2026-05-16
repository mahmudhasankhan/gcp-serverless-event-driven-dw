# Deploying FastAPI Upload Server to Cloud Run

## Overview

This document covers migrating the FastAPI file upload server from a locally hosted
uvicorn process to a fully serverless **Cloud Run** deployment on GCP. After completing
this section, the upload UI is permanently available at a public HTTPS URL — no local
server, no open terminals, no ngrok required.

This is **Part 1** of the full serverless migration:

| Part | Component | Status |
|------|-----------|--------|
| 1 | FastAPI upload server → Cloud Run | ✅ Complete |
| 2 | dbt jobs → Cloud Run Jobs (x3) | Upcoming |
| 3 | Airflow DAGs → Cloud Composer | Upcoming |
| 4 | Data quality → Dataplex | Upcoming |

---

## Architecture Before & After

**Before**
```
Browser → FastAPI (localhost:8000) → GCS bucket
              ↑
         uvicorn running locally
         terminal must stay open
```

**After**
```
Browser → FastAPI (Cloud Run HTTPS URL) → GCS bucket
              ↑
         fully managed, serverless
         scales to zero when idle
```

---

## Prerequisites

### 1. Google Cloud CLI

The `gcloud` CLI is required to deploy from your local machine.

**Install:** https://cloud.google.com/sdk/docs/install

- **Windows:** Download and run the `.exe` installer
- **macOS:** `brew install google-cloud-sdk` or download the tar archive
- **Linux:** Follow the apt/rpm instructions on the install page

**After installing, initialise:**

```bash
# Log in to your Google account
gcloud auth login

# Set your GCP project
gcloud config set project sales-datawarehouse

# Confirm it is working
gcloud projects list
```

### 2. Enable required GCP APIs

These APIs are needed for `gcloud run deploy --source .` to work. Run once per project:

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com
```

- **Cloud Run** — hosts the FastAPI service
- **Cloud Build** — builds the Docker image from source
- **Artifact Registry** — stores the built container image

---

## How `gcloud run deploy --source .` Works

This single command triggers an automated chain on GCP — you never touch Docker or
Cloud Build directly:

```
gcloud run deploy --source .
        │
        ▼
Cloud Build reads your Dockerfile
        │
        ▼
Builds the container image
        │
        ▼
Pushes image to Artifact Registry
        │
        ▼
Deploys image to Cloud Run
        │
        ▼
Returns a live HTTPS URL
```

Everything happens on GCP's infrastructure. Your local machine just sends the source
files and waits for the URL.

---

## Project Structure

Place all files inside your `web_portal/` folder before deploying:

```
web_portal/
├── main.py           ← existing, modified (see below)
├── index.html        ← existing, no changes
├── Dockerfile        ← new
├── requirements.txt  ← new
└── .dockerignore     ← new
```

---

## File Changes

### 1. Remove hardcoded values from `main.py`

Cloud Run uses **Application Default Credentials (ADC)** via an attached service
account — no JSON key file is needed inside the container. Remove all references to
the service account file path and replace the hardcoded bucket name with an
environment variable.

**Remove this pattern wherever it appears:**
```python
# DELETE these lines
SERVICE_ACCOUNT = "/usr/local/airflow/include/gcp/sales-upload-sa.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account
```

**Replace hardcoded bucket name:**
```python
# Before
GCS_BUCKET_NAME = "sales-dw-bucket"

# After
import os
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
```

**Update the upload function signature:**
```python
# Before — accepted service_account as a parameter
async def upload_file(file: UploadFile = File(...)):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT
    client = storage.Client()
    ...

# After — ADC handles credentials automatically
async def upload_file(file: UploadFile = File(...)):
    client = storage.Client()   # picks up the attached service account automatically
    ...
```

### 2. `Dockerfile`

```dockerfile
FROM python:3.12-slim

# Don't buffer stdout/stderr — important for Cloud Run logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY main.py .
COPY index.html .

# Cloud Run injects PORT env var — default 8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

> **Why `PYTHONUNBUFFERED=1`?** Without it, Python buffers stdout and your logs
> won't appear in Cloud Run's log viewer in real time.

> **Why `--host 0.0.0.0`?** By default uvicorn only listens on localhost (127.0.0.1),
> which is unreachable inside a container. `0.0.0.0` makes it listen on all interfaces
> so Cloud Run can route traffic to it.

### 3. `requirements.txt`

```text
fastapi>=0.110.0
uvicorn[standard]>=0.29.0
google-cloud-storage>=2.16.0
python-multipart>=0.0.9
```

### 4. `.dockerignore`

Prevents unnecessary files from being sent to Cloud Build:

```
__pycache__/
*.pyc
*.pyo
.env
.venv
venv/
*.egg-info/
.git/
.gitignore
```

---

## GCP Setup

### Step 1 — Create a dedicated service account

Rather than using the default Compute service account, create one scoped specifically
to this service:

```bash
gcloud iam service-accounts create fastapi-upload-sa \
  --display-name="FastAPI Upload Service Account"
```

### Step 2 — Grant it GCS write access

```bash
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:fastapi-upload-sa@sales-datawarehouse.iam.gserviceaccount.com" \
  --role="roles/storage.objectCreator"
```

`roles/storage.objectCreator` allows writing new objects to GCS but not reading or
deleting existing ones — principle of least privilege.

---

## Deployment

From inside your `web_portal/` folder:

```bash
cd web_portal

gcloud run deploy fastapi-upload \
  --source . \
  --region asia-south2 \
  --platform managed \
  --allow-unauthenticated \
  --service-account=fastapi-upload-sa@sales-datawarehouse.iam.gserviceaccount.com \
  --set-env-vars GCS_BUCKET_NAME=your-actual-bucket-name
```

**Flag breakdown:**

| Flag | Purpose |
|------|---------|
| `--source .` | Send current directory to Cloud Build |
| `--region asia-south2` | GCP region to deploy to |
| `--platform managed` | Fully managed Cloud Run (not GKE) |
| `--allow-unauthenticated` | Public access — anyone can open the upload UI |
| `--service-account` | Attach the SA we created above |
| `--set-env-vars` | Pass `GCS_BUCKET_NAME` into the container |

### What happens during deploy

Cloud Build streams its progress to your terminal:

```
Building and deploying from source...
Building Container...
✓ Building Container... Done.
✓ Pushing Container to Registry... Done.
✓ Creating Revision... Done.
✓ Routing traffic... Done.

Service URL: https://fastapi-upload-xxxx-as.a.run.app
```

The final URL is your permanent upload endpoint.

---

## Verifying the Deployment

### 1. Open the UI

Navigate to the Service URL in your browser. The drag-and-drop upload interface
should load exactly as it did locally.

### 2. Test the health endpoint

```bash
curl https://fastapi-upload-xxxx-as.a.run.app/health
# Expected: {"status":"ok","bucket":"your-bucket-name"}
```

### 3. Upload a test file

Use the UI or curl:

```bash
curl -X POST \
  "https://fastapi-upload-xxxx-as.a.run.app/upload" \
  -F "file=@test.xlsx"
```

### 4. Confirm the file landed in GCS

```bash
gcloud storage ls gs://your-bucket-name/
```

### 5. Check logs in GCP Console

Cloud Run → your service → Logs tab. Every upload request, GCS write, and any
errors appear here in real time.

---

## Environment Variable Management

The `GCS_BUCKET_NAME` env var is stored inside the Cloud Run service revision. To
view or update it without redeploying from source:

**Via GCP Console:**
Cloud Run → `fastapi-upload` → Edit & Deploy New Revision → Variables & Secrets

**Via CLI:**
```bash
# Update env var and deploy a new revision
gcloud run services update fastapi-upload \
  --region asia-south2 \
  --set-env-vars GCS_BUCKET_NAME=new-bucket-name
```

---

## Scaling & Cost

Cloud Run scales to **zero** when there are no requests — meaning you pay nothing
when the upload UI is idle. For a monthly file upload workflow, the cost is
effectively zero (well within the free tier of 2 million requests/month).

The default scaling config is `Min: 0, Max: 1` which is appropriate here. A file
upload UI does not need multiple concurrent instances.

---

## Key Concepts Learned

**Application Default Credentials (ADC)**
On Cloud Run, you attach a service account to the service instead of using a JSON
key file. The `google-cloud-storage` client automatically detects and uses these
credentials — no code change needed beyond removing the `GOOGLE_APPLICATION_CREDENTIALS`
line.

**Environment variables on Cloud Run**
Secrets and config values (bucket names, API URLs, etc.) are passed in at deploy
time via `--set-env-vars` and stored as part of the service revision. Never hardcode
these values in `main.py`.

**`gcloud run deploy --source .` vs `gcloud builds submit`**
`--source .` is an all-in-one command — build, push, and deploy in one step. Use
`gcloud builds submit` separately when you need to decouple the build from the
deploy, for example in a CI/CD pipeline where tests run between the two steps.

**Port binding**
Cloud Run injects a `PORT` environment variable and expects the container to listen
on it. The Dockerfile uses `--port 8080` which matches Cloud Run's default. The
`--host 0.0.0.0` flag is required so uvicorn accepts connections from outside the
container's loopback interface.

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `403 Forbidden` on upload | SA missing GCS permission | Re-run the `add-iam-policy-binding` command |
| `500 Internal Server Error` | `GCS_BUCKET_NAME` not set or wrong | Check env vars in Cloud Run console |
| Container fails to start | Port mismatch | Ensure `--port 8080` in CMD and `EXPOSE 8080` in Dockerfile |
| `google.auth.exceptions.DefaultCredentialsError` | SA not attached | Confirm `--service-account` flag was passed at deploy |
| Build fails | API not enabled | Run `gcloud services enable cloudbuild.googleapis.com` |

---

## Next Step

With the upload server live on Cloud Run, the next part of the serverless migration
is packaging the three dbt transformation jobs into Docker containers and deploying
them as **Cloud Run Jobs** — the GCP primitive designed for run-to-completion
workloads rather than persistent HTTP servers.


# Part 2 — dbt Transformations as Cloud Run Jobs

## Overview

This document covers migrating dbt transformations from a locally orchestrated
Airflow `DbtTaskGroup` to three independent **Cloud Run Jobs** deployed on GCP.
Each job runs a specific phase of the transformation pipeline inside a Docker
container, exits when complete, and reports success or failure via its exit code.

This is **Part 2** of the full serverless migration:

| Part | Component | Status |
|------|-----------|--------|
| 1 | FastAPI upload server → Cloud Run service | ✅ Complete |
| 2 | dbt transformations → Cloud Run Jobs (x3) | ✅ Complete |
| 3 | Airflow DAG → Cloud Composer | Upcoming |
| 4 | Data quality → Dataplex | Upcoming |

---

## Why Cloud Run Jobs (not Cloud Run Services)

Cloud Run has two primitives:

| | Cloud Run Service | Cloud Run Job |
|-|-------------------|---------------|
| Purpose | Long-running HTTP server | Run-to-completion workload |
| Triggered by | HTTP requests | Manually, on schedule, or via API |
| Exits when | Never (always listening) | Task completes or fails |
| Billed | Per request | Per execution time |
| Use case | FastAPI, APIs, web servers | dbt runs, batch jobs, scripts |

dbt is a run-to-completion workload — it runs, finishes, and exits. Cloud Run Jobs
are the correct primitive. Cloud Run Services would keep the container alive
indefinitely waiting for HTTP traffic, which makes no sense for dbt.

---

## Architecture

```
One Docker image per job (3 images total)
All images share the same dbt project files

dbt-staging image          dbt-transform image        dbt-marts-test image
Dockerfile.staging         Dockerfile.transform       Dockerfile.marts-test
        │                          │                          │
        ▼                          ▼                          ▼
dbt-staging-job            dbt-transform-job          dbt-marts-test-job
Cloud Run Job              Cloud Run Job              Cloud Run Job

Job 1 runs first → Job 2 runs if Job 1 passes → Job 3 runs if Job 2 passes
Orchestrated by Cloud Composer (Part 3)
```

---

## Key Design Decisions

### One image per job, not one image for all three

Each Dockerfile contains a different `CMD` — the only thing that differs between
the three images. A single shared image with a configurable entrypoint was
considered but rejected because:

- Separate images make each job independently deployable
- A broken mart test can be redeployed without touching the staging image
- Cloud Build history shows clearly which phase changed

### Staging model consolidated from two to one

The original project had two staging models:
- `stg_raw_sales.sql` — cast and trim columns
- `stg_sales.sql` — filter bad rows

These were collapsed into a single `stg_sales.sql` that casts, trims, and filters
in one model. Bad rows are not silently dropped — they are written to a quarantine
dataset before `stg_sales` runs.

### Quarantine as a `run-operation`, not inside a model

The quarantine macro was initially written to be called from inside a CTE in
`stg_sales.sql`. This caused a **maximum recursion depth exceeded** error during
dbt's Jinja parse phase — a known dbt limitation where `run_query()` cannot be
called inside a model file.

The fix: quarantine runs as a standalone `dbt run-operation quarantine_bad_rows`
step in the Dockerfile CMD, between the source test and the staging model run.

### `method: oauth` not `method: service-account` in profiles.yml

`method: service-account` in dbt-bigquery requires a `keyfile:` path pointing to
a JSON key file. Without it, dbt passes `None` as the path which causes a
`expected str, bytes or os.PathLike object, not NoneType` error.

On Cloud Run, the attached service account is picked up automatically via
Application Default Credentials (ADC). The correct dbt profile method for ADC
is `method: oauth` — no keyfile needed, no JSON file in the container.

### `quote: false` on integer `accepted_values` tests

dbt's `accepted_values` test wraps all values in quotes by default, producing
`column IN ('1','2','3')`. BigQuery rejects this when the column type is `INT64`.
Adding `quote: false` produces `column IN (1,2,3)` which BigQuery accepts.

Affected columns: `dim_date.month`, `dim_date.quarter`, `dim_date.day_of_week`,
`fct_grocery_sales.sale_count`, `quarterly_revenue_growth.quarter`.

---

## Project Structure

Place all files in a `dbt-jobs/` folder alongside your existing dbt project files:

```
dbt-jobs/
├── dbt_project.yml              ← updated (quarantine config removed from pre-hooks)
├── profiles.yml                 ← new (method: oauth for ADC)
├── packages.yml                 ← existing (dbt_utils 1.3.2)
├── requirements.txt             ← new (dbt-core + dbt-bigquery)
├── .dockerignore                ← new
│
├── Dockerfile.staging           ← Job 1
├── Dockerfile.transform         ← Job 2
├── Dockerfile.marts-test        ← Job 3
│
├── cloudbuild.staging.yml       ← build + deploy Job 1
├── cloudbuild.transform.yml     ← build + deploy Job 2
├── cloudbuild.marts-test.yml    ← build + deploy Job 3
│
├── models/
│   ├── sources.yml              ← updated with source tests + freshness
│   ├── staging/
│   │   ├── stg_sales.sql        ← new single model (replaces stg_raw_sales + stg_sales)
│   │   └── schema.yml           ← updated
│   └── marts/
│       ├── dimension/
│       ├── fact/
│       ├── rankings/
│       ├── ytd_mtd_qtd_growth/
│       └── schema.yml           ← new (generic + singular tests for all mart models)
│
├── macros/
│   ├── generate_schema_name.sql ← existing
│   └── quarantine_bad_rows.sql  ← new
│
└── tests/
    ├── assert_fct_positive_financial_values.sql
    ├── assert_fct_no_orphaned_foreign_keys.sql
    ├── assert_fct_revenue_calculation_consistent.sql
    ├── assert_rankings_positive_revenue.sql
    ├── assert_ytd_revenue_gte_daily.sql
    └── assert_monthly_revenue_no_duplicates.sql
```

> **Delete `stg_raw_sales.sql`** from your project before deploying. It no longer
> exists and any dbt reference to it will cause a parse error.

---

## Files

### `profiles.yml`

```yaml
sales_dw_pipeline:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth                  # ADC — uses Cloud Run attached SA automatically
      project: "{{ env_var('DBT_PROJECT', 'sales-datawarehouse') }}"
      dataset: "{{ env_var('DBT_DATASET', 'staging') }}"
      location: "{{ env_var('DBT_LOCATION', 'asia-south2') }}"
      threads: 4
      job_execution_timeout_seconds: 7200
      job_retries: 1
```

### `requirements.txt`

```
dbt-core==1.8.0
dbt-bigquery==1.8.0
```

### `Dockerfile.staging` (Job 1)

```dockerfile
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml packages.yml profiles.yml ./
COPY models/ models/
COPY macros/ macros/
COPY tests/ tests/

CMD ["sh", "-c", \
     "dbt deps && \
      dbt source freshness && \
      dbt test --select source:raw_data && \
      dbt run-operation quarantine_bad_rows && \
      dbt run  --select staging && \
      dbt test --select staging"]
```

### `Dockerfile.transform` (Job 2)

```dockerfile
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml packages.yml profiles.yml ./
COPY models/ models/
COPY macros/ macros/

CMD ["sh", "-c", \
     "dbt deps && \
      dbt run --select marts"]
```

### `Dockerfile.marts-test` (Job 3)

```dockerfile
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml packages.yml profiles.yml ./
COPY models/ models/
COPY macros/ macros/
COPY tests/ tests/

CMD ["sh", "-c", \
     "dbt deps && \
      dbt test --select marts"]
```

---

## GCP Setup

### 1. Enable required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com
```

### 2. Grant Cloud Run execution permissions to existing SA

Your existing SA `bigquery-service@sales-datawarehouse.iam.gserviceaccount.com`
already has `bigquery.dataEditor` and `bigquery.jobUser`. Add one more role so it
can be attached to Cloud Run Jobs:

```bash
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:bigquery-service@sales-datawarehouse.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
```

> `roles/bigquery.readSessionUser` is NOT needed. That role is only for the
> BigQuery Storage Read API used by Spark/pandas. dbt submits SQL jobs entirely
> server-side inside BigQuery and never needs it.

### 3. Create the quarantine dataset

The quarantine macro writes to this dataset but will not create it:

```bash
bq mk --dataset sales-datawarehouse:quarantine
```

---

## Quarantine Logic

### How it works

Bad rows from `raw_data.sales` are written to a permanent BigQuery table before
the staging model runs. The quarantine macro is called as a standalone
`dbt run-operation` — not inside the model — to avoid dbt's Jinja recursion
limitation with `run_query()` inside model files.

### Quarantine conditions

A row is quarantined if ANY of these are true:

| Condition | Reason |
|-----------|--------|
| `order_id IS NULL` | Can't identify the record |
| `customer_id IS NULL` | Breaks dim_customer surrogate key |
| `unit_price IS NULL OR <= 0` | Invalid financial data |
| `quantity IS NULL OR <= 0` | Invalid financial data |
| `revenue IS NULL OR < 0` | Invalid financial data |
| `shipped_date < order_date` | Logically impossible |

### What gets written to BigQuery

Table name: `quarantine.stg_sales_rejected_YYYYMMDD`

Every bad row is written with two extra columns:
- `quarantined_at` — timestamp of when the row was quarantined
- `failed_condition` — the exact rule that caused rejection

Example quarantine table:

| order_id | unit_price | failed_condition | quarantined_at |
|----------|------------|-----------------|----------------|
| 1042 | NULL | null unit_price | 2025-04-01 08:32:11 |
| 1089 | 120.00 | shipped before ordered | 2025-04-01 08:32:11 |

### What flows downstream

Only rows where ALL quality conditions pass reach `stg_sales`. Every dimension
table, the fact table, and all mart models downstream only ever see clean data.

---

## Source Tests and Severity Levels

Source tests in `sources.yml` run directly against `raw_data.sales` before any
model runs. Two severity levels are used:

**`severity: error` — pipeline stops immediately:**

| Column | Reason |
|--------|--------|
| `order_id` | Can't identify any record without it |
| `customer_id` | Breaks dim_customer surrogate key generation |
| `product_name` | Breaks dim_product surrogate key generation |
| `order_date` | Can't join dim_date for order_date_key |
| `batch_id` | Null means Cloud Run load function had a bug |
| `source_file_name` | Null means Cloud Run load function had a bug |
| `loaded_at` | Required for freshness check |

**`severity: warn` — pipeline continues, quarantine handles it:**

| Column | Reason |
|--------|--------|
| `unit_price`, `quantity` | 3 null rows in real data — quarantine isolates them |
| `revenue`, `shipping_fee` | Business value nulls — quarantine handles |
| `region`, `payment_type` | Unexpected values trigger investigation but don't block pipeline |

### Source freshness

```yaml
freshness:
  warn_after:  { count: 40, period: day }
  error_after: { count: 60, period: day }
loaded_at_field: loaded_at
```

Checks the most recent `loaded_at` timestamp in `raw_data.sales`. Warns after
40 days (monthly file + buffer), errors after 60 days. Run via
`dbt source freshness` as the first step in Job 1.

---

## Data Tests

### Generic tests (`schema.yml`)

Defined on every model column. Key patterns:

```yaml
# Surrogate key — must be unique and not null on every dimension
- name: customer_key
  tests:
    - unique
    - not_null

# Foreign key — must resolve to a dimension record
- name: customer_key   # in fct_grocery_sales
  tests:
    - relationships:
        to: ref('dim_customer')
        field: customer_key

# Integer accepted values — quote: false required for BigQuery INT64
- name: quarter
  tests:
    - accepted_values:
        values: [1,2,3,4]
        quote: false        # without this BigQuery rejects INT64 IN ('1','2','3','4')
```

### Singular tests (`tests/`)

Six custom SQL tests that catch things generic tests cannot:

| Test file | What it catches |
|-----------|----------------|
| `assert_fct_positive_financial_values.sql` | Negative revenue/price/quantity in fact |
| `assert_fct_no_orphaned_foreign_keys.sql` | FKs in fact that don't match any dimension |
| `assert_fct_revenue_calculation_consistent.sql` | Revenue ≠ unit_price × quantity |
| `assert_rankings_positive_revenue.sql` | Zero/null revenue or null rank in ranking models |
| `assert_ytd_revenue_gte_daily.sql` | YTD cumulative < daily — broken window function |
| `assert_monthly_revenue_no_duplicates.sql` | Duplicate year+month — broken GROUP BY |

---

## Deployment

### How `$PROJECT_ID` works in `cloudbuild.yml`

`$PROJECT_ID` is a built-in Cloud Build substitution variable injected automatically
from your active `gcloud` project. It works in `name:` fields and `images:` fields
but **not inside `args:` values**. Any project ID reference inside `args:` must be
hardcoded.

```yaml
# ✅ $PROJECT_ID works — in name: and images:
- name: "gcr.io/$PROJECT_ID/dbt-staging"
images:
  - "gcr.io/$PROJECT_ID/dbt-staging"

# ❌ $PROJECT_ID does NOT work — inside args: values
# must hardcode:
"--service-account=bigquery-service@sales-datawarehouse.iam.gserviceaccount.com"
"--set-env-vars=DBT_PROJECT=sales-datawarehouse,..."
```

Confirm your active project before submitting:

```bash
gcloud config get-value project
# should return: sales-datawarehouse
```

### Deploy all three jobs

Run each from inside your `dbt-jobs/` folder. Deploy and test one at a time:

```bash
cd dbt-jobs

# Job 1 — staging
gcloud builds submit --config cloudbuild.staging.yml .

# Job 2 — transform (only after Job 1 executes successfully)
gcloud builds submit --config cloudbuild.transform.yml .

# Job 3 — marts test (only after Job 2 executes successfully)
gcloud builds submit --config cloudbuild.marts-test.yml .
```

Each command triggers Cloud Build to:
1. Build the Docker image using the specified Dockerfile
2. Push the image to Artifact Registry — overwrites previous image with same tag
3. Deploy or update the Cloud Run Job pointing to the new image

Redeploying is always the same command — no cleanup needed.

### Manually execute a job

```bash
# Execute Job 1
gcloud run jobs execute dbt-staging-job --region asia-south2

# Execute Job 2
gcloud run jobs execute dbt-transform-job --region asia-south2

# Execute Job 3
gcloud run jobs execute dbt-marts-test-job --region asia-south2
```

### Check execution status and logs

```bash
# List executions for a job
gcloud run jobs executions list --job=dbt-staging-job --region asia-south2

# Stream logs
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=dbt-staging-job" \
  --limit=100 \
  --format="value(textPayload)" \
  --order=asc \
  --project=sales-datawarehouse
```

Or via GCP Console: **Cloud Run → Jobs → select job → Executions → select
execution → Logs**

---

## Job 1 Execution Walkthrough

When `dbt-staging-job` executes, the following runs in sequence inside the container:

```
Phase 1 — dbt deps
  Installs dbt_utils 1.3.2 from packages.yml
  Required for generate_surrogate_key used in all dimension and fact models

Phase 2 — dbt source freshness
  Queries MAX(loaded_at) from raw_data.sales
  WARN  if last load > 40 days ago
  ERROR if last load > 60 days ago → job stops here

Phase 3 — dbt test --select source:raw_data
  Runs all tests defined in sources.yml against raw_data.sales directly
  ERROR severity tests  → job stops, pipeline blocked
  WARN  severity tests  → logged but job continues
  Real example: 3 null unit_price rows → WARN, continues to Phase 4

Phase 4 — dbt run-operation quarantine_bad_rows
  Counts bad rows in raw_data.sales matching quality conditions
  If bad rows exist → writes quarantine.stg_sales_rejected_YYYYMMDD to BigQuery
  Logs: "3 bad rows found — writing to sales-datawarehouse.quarantine.stg_sales_rejected_20250401"
  If no bad rows → logs "No bad rows found — quarantine skipped"

Phase 5 — dbt run --select staging
  Materialises stg_sales as a VIEW in BigQuery staging dataset
  stg_sales filters WHERE quality_issue IS NULL — only clean rows
  Bad rows already captured in quarantine table in Phase 4

Phase 6 — dbt test --select staging
  Runs all tests in models/staging/schema.yml against stg_sales
  Should all pass — quarantine already removed bad rows
  If a test fails → job exits code 1 → Composer stops pipeline (Part 3)
```

**Exit code 0** → Cloud Composer proceeds to Job 2
**Exit code 1** → Cloud Composer stops, sends alert email, Job 2 never runs

---

## BigQuery State After All Three Jobs

```
BigQuery
├── raw_data
│   └── sales                          ← 369 rows, untouched source of truth
│
├── quarantine
│   └── stg_sales_rejected_20250401    ← 3 bad rows with failed_condition column
│
├── staging
│   └── stg_sales                      ← VIEW, 366 clean rows
│
└── warehouse  (marts schema)
    ├── dim_customer
    ├── dim_product
    ├── dim_salesperson
    ├── dim_shipper
    ├── dim_region
    ├── dim_payment_type
    ├── dim_date
    ├── fct_grocery_sales
    ├── monthly_revenue_growth
    ├── quarterly_revenue_growth
    ├── yearly_revenue_growth
    ├── revenue_qty_sales_trend_by_year_quarter_month
    ├── shipment_exp_comparison_by_ship_company
    ├── city_rank_by_revenue_qty_sales
    ├── customer_rank_by_revenue_qty_sales
    ├── product_rank_by_revenue_qty_sales
    ├── region_rank_by_revenue_qty_sales
    ├── salesperson_rank_by_revenue_qty_sales
    ├── ytd_revenue_qty_shipment_growth
    ├── mtd_revenue_qty_shipment_growth
    └── qtd_revenue_qty_shipment_growth
```

---

## Errors Encountered and Fixes

### Error 1 — Maximum recursion depth exceeded

**When:** Job 1 first execution, during dbt parse phase

**Cause:** `quarantine_bad_rows` macro called `run_query()` inside a CTE in
`stg_sales.sql`. dbt's Jinja parser recurses infinitely trying to resolve
`run_query()` at parse time before any SQL runs.

**Fix:** Moved quarantine logic out of the model entirely into a standalone
`dbt run-operation quarantine_bad_rows` command in the Dockerfile CMD. The model
(`stg_sales.sql`) now only filters rows — no macro calls.

**Rule learned:** `run_query()` can only be called from `run-operation`, hooks,
or other macros — never directly inside a model `.sql` file.

### Error 2 — NoneType error on source freshness

**When:** Job 1 second execution, during `dbt source freshness`

**Cause:** `profiles.yml` used `method: service-account` which requires a
`keyfile:` path. Without a path dbt passes `None`, causing
`expected str, bytes or os.PathLike object, not NoneType`.

**Fix:** Changed `method: service-account` to `method: oauth`. On Cloud Run,
the attached service account is picked up automatically via ADC. `method: oauth`
is the correct choice when no JSON key file is present.

### Error 3 — Source test failures blocking the pipeline

**When:** Job 1 third execution, `dbt test --select source:raw_data`

**Cause:** 3 rows in `raw_data.sales` had null `unit_price` and `quantity`. All
source tests defaulted to `severity: error`, causing the job to exit with code 1.

**Fix:** Changed financial column tests (`unit_price`, `quantity`, `revenue`,
`shipping_fee`) to `severity: warn`. These are handled by the quarantine macro —
the pipeline should continue and quarantine the rows rather than stopping entirely.
Hard structural fields (`order_id`, `customer_id`, `batch_id`) remain
`severity: error`.

### Error 4 — INT64 IN STRING type mismatch (marts test)

**When:** Job 3 first execution, `dbt test --select marts`

**Cause:** dbt's `accepted_values` test quotes all values by default.
`values: [1,2,3,4]` becomes `column IN ('1','2','3','4')`. BigQuery rejects
comparing `INT64` to `STRING`.

**Fix:** Added `quote: false` to all `accepted_values` tests on integer columns.

### Error 5 — Unrecognized column name in shipment model

**When:** Job 3 first execution, `dbt test --select marts`

**Cause:** `schema.yml` referenced `total_shipping_fee` but the actual column
in the model is `total_shipping_fees` (with trailing `s`).

**Fix:** Corrected column name in `schema.yml`.

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `maximum recursion depth exceeded` | `run_query()` inside a model CTE | Move to `run-operation` |
| `NoneType` on freshness | Wrong auth method in profiles.yml | Use `method: oauth` for ADC |
| `INT64 IN STRING` type error | Integer `accepted_values` without `quote: false` | Add `quote: false` |
| `Unrecognized name: <column>` | Column name in schema.yml doesn't match model | Check actual column name in SQL |
| Source test blocks pipeline | All tests defaulting to `severity: error` | Use `severity: warn` for quarantined fields |
| `roles/bigquery.readSessionUser` error | Not needed — don't add it | dbt uses Jobs API, not Storage Read API |

---

## Next Step

With all three Cloud Run Jobs deployed and running cleanly, the next part is
wiring them together in **Cloud Composer** — replacing the local Airflow setup
with a fully managed DAG that triggers Job 1 → Job 2 → Job 3 in sequence,
uses a branch operator to stop on failure, and sends email alerts.


# Part 2.5 — Cloud Run Extract-Load Function

## Overview

This document covers the **extract-load Cloud Run service** — the component that
sits between the FastAPI upload server and Cloud Composer. When a monthly Excel
file lands in GCS, this function is triggered automatically, extracts and cleans
the data, loads it into BigQuery, and triggers the Composer DAG to start the dbt
transformation pipeline.

This sits between Part 2 (dbt Cloud Run Jobs) and Part 3 (Cloud Composer) in the
migration sequence:

| Part | Component | Status |
|------|-----------|--------|
| 1 | FastAPI upload server → Cloud Run service | ✅ Complete |
| 2 | dbt transformations → Cloud Run Jobs (x3) | ✅ Complete |
| **2.5** | **Extract-load function → Cloud Run service** | ✅ Complete |
| 3 | Airflow DAG → Cloud Composer 3 | In progress |
| 4 | Data quality → Dataplex Universal Catalog | Upcoming |

---

## What This Function Does

```
GCS bucket (Sales Data.xlsx lands)
        │  OBJECT_FINALIZE event → Eventarc
        ▼
Cloud Run function (extract-load)
        │
        ├── 1. Validate file is Excel (.xlsx / .xls)
        ├── 2. Download file from GCS into memory
        ├── 3. Clean and normalise column names
        ├── 4. Add metadata columns (batch_id, source_file_name, loaded_at)
        ├── 5. Convert date columns to Python date objects (BigQuery compliance)
        ├── 6. Dedup check — skip load if batch already exists in BigQuery
        ├── 7. Load clean DataFrame into raw_data.sales in BigQuery
        └── 8. Trigger stratum_elt_pipeline DAG via Composer REST API
```

---

## Project Structure

```
cloud_run/
├── main.py          ← the function code
└── requirements.txt ← dependencies
```

The Cloud Run function is a **service** (not a Job) — it listens for incoming
HTTP events from Eventarc and processes them. It is separate from the three
dbt Cloud Run Jobs which are run-to-completion workloads.

---

## The Function — `main.py`

### Full annotated code

```python
import functions_framework
import requests
import pandas as pd
import logging
import re

from io import BytesIO
from pathlib import Path
from datetime import datetime, UTC

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
import google.auth
from google.auth.transport.requests import AuthorizedSession

# ── Config ────────────────────────────────────────────────────────────────────
TABLE_ID       = "sales-datawarehouse.raw_data.sales"
WEB_SERVER_URL = "https://YOUR-COMPOSER-WEBSERVER-URL"   # get from gcloud CLI
DAG_ID         = "stratum_elt_pipeline"
ENDPOINT       = f"api/v1/dags/{DAG_ID}/dagRuns"
# NOTE: /api/v1/ because Composer 3 runs Airflow 2.x
# Switch to /api/v2/ only when Composer ships Airflow 3.x

logger = logging.getLogger(__name__)

# ── Auth — initialised at module level (GCP best practice) ────────────────────
# Creating credentials at module level means they are initialised once per
# container instance and reused across invocations — not re-created per request.
AUTH_SCOPE  = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])


# ── URL validation — runs at container startup ────────────────────────────────
def _validate_composer_url(url: str) -> str:
    """
    Validates the Composer webserver URL at container startup.
    Catches common copy-paste mistakes before they cause confusing
    runtime errors mid-execution.

    Common mistakes:
      - Double https:// from copy-pasting
      - /api/ path fragment accidentally included in the base URL
      - Trailing slash causing double slashes in constructed endpoint
    """
    if url.count("https://") > 1:
        raise ValueError(f"WEB_SERVER_URL contains double https:// — {url}")
    if "api/" in url:
        raise ValueError(f"WEB_SERVER_URL must be base URL only, not include /api/: {url}")
    if not url.startswith("https://"):
        raise ValueError(f"WEB_SERVER_URL must start with https://: {url}")
    if not re.search(r'composer\.googleusercontent\.com', url):
        raise ValueError(f"WEB_SERVER_URL does not look like a valid Composer URL: {url}")
    return url.rstrip("/")   # normalise — remove trailing slash

WEB_SERVER_URL = _validate_composer_url(WEB_SERVER_URL)


# ── Composer DAG trigger ──────────────────────────────────────────────────────
def _trigger_composer_dag(filename: str, bucket: str) -> str | None:
    """
    Triggers stratum_elt_pipeline in Cloud Composer via the Airflow REST API.

    Uses AuthorizedSession which:
      - Handles Google OAuth2 token generation automatically
      - Refreshes tokens when they expire (1 hour) without extra code
      - Uses the Cloud Run function's attached SA via ADC

    Returns the dag_run_id on success, None on failure.
    """
    request_url = f"{WEB_SERVER_URL}/{ENDPOINT}"
    logger.info(f"Triggering Composer DAG at: {request_url}")

    payload = {
        "logical_date": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conf": {
            "filename": filename,
            "bucket":   bucket,
        },
    }

    try:
        authed_session = AuthorizedSession(CREDENTIALS)
        response = authed_session.request(
            method="POST",
            url=request_url,
            json=payload,
            timeout=30,    # prevent the function hanging on a slow connection
        )

        # Separate handling for Airflow RBAC failures
        # IAM token can be valid but Airflow's internal RBAC can still deny
        if response.status_code == 403:
            logger.error(
                "IAM authentication succeeded but Airflow RBAC denied the request. "
                "Ensure the SA has roles/composer.user. "
                f"Details: {response.text}"
            )
            return None

        response.raise_for_status()

        dag_run_id = response.json().get("dag_run_id")
        logger.info(f"DAG triggered successfully — dag_run_id: {dag_run_id}")
        return dag_run_id

    except requests.exceptions.Timeout:
        logger.error("Connection to Composer webserver timed out after 30s.")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error triggering DAG: {e} | Response: {response.text}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error triggering DAG: {str(e)}")
        return None


# ── Main entry point ──────────────────────────────────────────────────────────
@functions_framework.cloud_event
def extract_and_load(cloud_event):
    data     = cloud_event.data
    filename = data["name"]
    bucket   = data["bucket"]

    if not filename.endswith((".xlsx", ".xls")):
        logger.info(f"Skipped non-Excel file: {filename}")
        return "Skipped", 200

    try:
        # ── Extract ───────────────────────────────────────────────────────────
        logger.info(f"Downloading {filename} from {bucket}")
        blob = storage.Client().bucket(bucket).blob(filename)
        df   = pd.read_excel(BytesIO(blob.download_as_bytes()))

        # ── Clean ─────────────────────────────────────────────────────────────
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r'[ /]+', '_', regex=True)
            .str.replace(r'[^0-9a-zA-Z_]', '', regex=True)
        )

        BATCH_ID = datetime.now(UTC).strftime("batch_%Y_%m")
        df['source_file_name'] = Path(filename).name
        df['batch_id']         = BATCH_ID
        df['loaded_at']        = datetime.now(UTC)

        # ── Date conversion ───────────────────────────────────────────────────
        # pandas reads Excel dates as datetime64[ns] objects
        # BigQuery DATE schema fields require Python datetime.date objects
        # without this conversion the load job raises a schema mismatch error
        for col in ['order_date', 'shipped_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date

        # ── Dedup check ───────────────────────────────────────────────────────
        bq_client = bigquery.Client()
        try:
            result = bq_client.query(
                f"SELECT COUNT(1) AS count FROM `{TABLE_ID}` "
                f"WHERE batch_id = '{BATCH_ID}'"
            ).result()

            if list(result)[0].count > 0:
                logger.warning(
                    f"Batch {BATCH_ID} already loaded — "
                    f"skipping BQ load, triggering DAG anyway."
                )
                _trigger_composer_dag(filename, bucket)
                return "Duplicate batch — DAG triggered", 200

        except NotFound:
            logger.info("Table not found — will be created on first load.")

        # ── Load ──────────────────────────────────────────────────────────────
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("order_id",            "INTEGER",   mode="REQUIRED"),
                bigquery.SchemaField("order_date",          "DATE",      mode="NULLABLE"),
                bigquery.SchemaField("customer_id",         "INTEGER",   mode="NULLABLE"),
                bigquery.SchemaField("customer_name",       "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("city",                "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("state",               "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("country_region",      "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("salesperson",         "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("region",              "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("shipped_date",        "DATE",      mode="NULLABLE"),
                bigquery.SchemaField("shipper_name",        "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("ship_name",           "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("ship_address",        "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("ship_city",           "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("ship_state",          "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("ship_country_region", "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("payment_type",        "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("product_name",        "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("category",            "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("unit_price",          "FLOAT64",   mode="NULLABLE"),
                bigquery.SchemaField("quantity",            "FLOAT64",   mode="NULLABLE"),
                bigquery.SchemaField("revenue",             "FLOAT64",   mode="NULLABLE"),
                bigquery.SchemaField("shipping_fee",        "FLOAT64",   mode="NULLABLE"),
                bigquery.SchemaField("revenue_bins",        "FLOAT64",   mode="NULLABLE"),
                bigquery.SchemaField("source_file_name",    "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("batch_id",            "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("loaded_at",           "TIMESTAMP", mode="REQUIRED"),
            ],
            write_disposition="WRITE_APPEND",
        )

        logger.info(f"Loading data to BigQuery: {TABLE_ID}")
        job = bq_client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()
        logger.info(f"BigQuery load complete.")

        # ── Trigger Composer ──────────────────────────────────────────────────
        dag_run_id = _trigger_composer_dag(filename, bucket)
        if dag_run_id is None:
            logger.error(
                "Data loaded to BigQuery successfully but "
                "Composer DAG failed to trigger — check logs."
            )

        return "Processing complete", 200

    except Exception as err:
        # Always return 200 — non-200 causes Eventarc to retry the event
        # which would attempt to reload the same file into BigQuery
        logger.critical(f"Pipeline crashed: {str(err)}", exc_info=True)
        return "Internal failure intercepted", 200
```

---

## `requirements.txt`

```txt
functions-framework==3.*
requests>=2.31.0
pandas>=2.0.0
openpyxl>=3.1.0
google-cloud-bigquery>=3.11.0
google-cloud-storage>=2.16.0
pyarrow>=15.0.0
google-auth>=2.28.0
```

`openpyxl` — required by pandas to read `.xlsx` files.
`pyarrow` — required by `load_table_from_dataframe()` to serialise the DataFrame.
`google-auth` — required for `google.auth.default()` and `AuthorizedSession`.

---

## Service Account Permissions

The function uses `bigquery-service@sales-datawarehouse.iam.gserviceaccount.com`
attached to the Cloud Run service via `--service-account` at deploy time.

Full set of roles required:

| Role | Purpose | When added |
|------|---------|------------|
| `roles/storage.objectCreator` | Write uploads to GCS (FastAPI) | Part 1 |
| `roles/storage.objectViewer` | Read the uploaded Excel file from GCS | Added during debugging |
| `roles/bigquery.dataEditor` | Write to `raw_data.sales` | Existing |
| `roles/bigquery.jobUser` | Submit BigQuery load and query jobs | Existing |
| `roles/composer.user` | Call Composer REST API to trigger DAGs | Added during debugging |

```bash
# Add the two roles discovered during debugging
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:bigquery-service@sales-datawarehouse.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:bigquery-service@sales-datawarehouse.iam.gserviceaccount.com" \
  --role="roles/composer.user"
```

---

## Getting the Composer Webserver URL

Always use the CLI — never copy from the browser:

```bash
gcloud composer environments describe stratum-composer \
  --location=asia-south2 \
  --format="value(config.airflowUri)" \
  --project=sales-datawarehouse
```

Output:
```
https://88d2e9eba36448fbad243b7923dfbd8b-dot-asia-south2.composer.googleusercontent.com
```

Paste this directly into `WEB_SERVER_URL` in `main.py`. The URL validation
function catches the most common copy-paste mistakes at container startup.

---

## Deployment

```bash
cd cloud_run

gcloud run deploy extract-load \
  --source . \
  --region asia-south2 \
  --no-allow-unauthenticated \
  --service-account=bigquery-service@sales-datawarehouse.iam.gserviceaccount.com \
  --set-env-vars GCS_BUCKET_NAME=sales-dw-bucket \
  --memory=1Gi
```

`--memory=1Gi` — pandas loading a full Excel file into memory requires at least
1GB. The default 512MB can cause the container to OOM-crash silently.

`--no-allow-unauthenticated` — the function is triggered by Eventarc, not
public HTTP traffic. Eventarc handles authentication automatically.

After deploy, wire the Eventarc trigger:

```bash
gcloud run deploy extract-load \
  --source . \
  --region asia-south2 \
  --no-allow-unauthenticated \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=sales-dw-bucket"
```

---

## Key Design Decisions

### `AuthorizedSession` over manual token fetching

The initial implementation fetched a Google ID token manually per invocation.
`AuthorizedSession` is cleaner because it handles token refresh automatically —
if the container stays warm between monthly runs (unlikely but possible) and the
1-hour token expires, it refreshes without any extra code. It also follows the
GCP-recommended pattern for server-to-server calls.

### Credentials initialised at module level

```python
# ✅ module level — once per container instance
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

# ❌ function level — re-created on every invocation
def extract_and_load(cloud_event):
    credentials, _ = google.auth.default(scopes=[AUTH_SCOPE])
```

GCP best practice — credentials are a heavy object. Creating them at module
level means they're initialised once when the container starts and reused
for all invocations that hit the same container instance.

### Always return 200

Eventarc uses HTTP status codes to decide whether to retry. If the function
returns anything other than 200, Eventarc retries the event — which in this
case means attempting to load the same file into BigQuery again, creating
duplicate rows. Always return 200 and surface errors through Cloud Logging with
`logger.critical(..., exc_info=True)` which includes the full stack trace.

### Date conversion before BigQuery load

```python
for col in ['order_date', 'shipped_date']:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col]).dt.date
```

pandas reads Excel date cells as `datetime64[ns]` — a numpy type. BigQuery's
`DATE` schema field rejects this type and expects Python's `datetime.date`.
Without this conversion the load job raises a schema mismatch error. Converting
via `.dt.date` produces the correct Python objects BigQuery accepts.

### Dedup check triggers DAG even on duplicate

```python
if list(result)[0].count > 0:
    logger.warning(f"Batch {BATCH_ID} already loaded — triggering DAG anyway.")
    _trigger_composer_dag(filename, bucket)
    return "Duplicate batch — DAG triggered", 200
```

If the batch was already loaded but the DAG wasn't triggered (e.g. Composer
was temporarily down), the function still triggers the DAG. This prevents a
scenario where data is in BigQuery but transformations never ran.

---

## API Version Note

```python
ENDPOINT = f"api/v1/dags/{DAG_ID}/dagRuns"
```

`/api/v1/` is correct for **Composer 3 running Airflow 2.x**.

This caused confusion because the local Astro CLI setup used Airflow 3.x
which exposes `/api/v2/`. These are independent version numbers:

| Environment | Airflow version | Correct API endpoint |
|-------------|----------------|---------------------|
| Local Astro CLI | Airflow 3.x | `/api/v2/` |
| Cloud Composer 3 | Airflow 2.x | `/api/v1/` |

Verify your Composer environment's Airflow version before constructing the URL:

```bash
gcloud composer environments describe stratum-composer \
  --location=asia-south2 \
  --format="value(config.softwareConfig.imageVersion)"
# composer-3.0.0-airflow-2.9.3 → use /api/v1/
```

---

## Errors Encountered and Fixes

### Error 1 — `storage.objects.get` permission denied

```
bigquery-service@... does not have storage.objects.get access
to the Google Cloud Storage object
```

**Cause:** SA had `roles/storage.objectCreator` (write-only). Reading the
uploaded file back requires `roles/storage.objectViewer`.

**Fix:** Added `roles/storage.objectViewer` to the SA.

### Error 2 — DNS resolution failure / connection error to Composer

```
socket.gaierror: [Errno -3] Temporary failure in name resolution
```

**Cause:** SA was missing `roles/composer.user`. The connection was being
rejected at the IAM layer before reaching the Composer webserver, surfacing
as a DNS/connection error rather than a clean 403.

**Fix:** Added `roles/composer.user` to the SA.

### Error 3 — Malformed Composer webserver URL

**Cause:** Browser copy-paste introduced:
- Double `https://https://` prefix
- API path fragment in the base URL string (`/api/ag/v1` appended)

**Fix:** Always use `gcloud ... --format="value(config.airflowUri)"` to
get the URL. Added `_validate_composer_url()` to catch this at startup.

### Error 4 — 5xx unreachable errors in Cloud Run metrics

**Cause:** Container running out of memory (OOM) when loading large Excel
files — pandas requires significant memory for DataFrame operations.

**Fix:** Set `--memory=1Gi` on the Cloud Run service deployment.

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `storage.objects.get` denied | Missing `storage.objectViewer` | Add role to SA |
| DNS / name resolution failure | Missing `composer.user` or bad URL | Add role + validate URL |
| `403` on DAG trigger | RBAC denied — SA not authorised | Confirm `roles/composer.user` |
| `404` on DAG trigger | Wrong API version or DAG ID mismatch | Check Airflow version, check DAG ID |
| Schema mismatch on BQ load | Date columns not converted | Add `.dt.date` conversion |
| OOM crash in Cloud Run | Not enough memory for pandas | Set `--memory=1Gi` |
| Eventarc retrying endlessly | Function returning non-200 on error | Always return 200, log errors |
| Duplicate rows in BigQuery | Dedup check not running | Verify `batch_id` column is populated |
