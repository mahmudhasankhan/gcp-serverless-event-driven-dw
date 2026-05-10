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
