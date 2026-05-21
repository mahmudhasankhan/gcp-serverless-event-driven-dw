# Part 3 — Cloud Composer 3 Orchestration

## Overview

This document covers migrating the Airflow DAG from a locally managed Astro CLI
setup exposed via ngrok to a fully managed **Cloud Composer 3** environment on GCP.
After completing this section the pipeline is entirely serverless — no local machine
involvement at any stage.

This is **Part 3** of the full serverless migration:

| Part | Component | Status |
|------|-----------|--------|
| 1 | FastAPI upload server → Cloud Run service | ✅ Complete |
| 2 | dbt transformations → Cloud Run Jobs (x3) | ✅ Complete |
| 2.5 | Extract-load function → Cloud Run service | ✅ Complete |
| 3 | Airflow DAG → Cloud Composer 3 | ✅ Complete |
| 4 | Data quality → Dataplex Universal Catalog | Upcoming |

---

## Architecture

```
GCS bucket (monthly Excel file lands)
        │  OBJECT_FINALIZE → Eventarc
        ▼
Cloud Run function (extract-load)
        │  AuthorizedSession → Composer REST API
        │  POST /api/v1/dags/transformation_pipeline/dagRuns
        ▼
Cloud Composer 3 (Airflow 2.x)
DAG: transformation_pipeline  (schedule=None)
        │
        ├── start
        │
        ├── test_raw_data_group
        │     ├── execute_staging_job    (dbt-staging-job Cloud Run Job)
        │     ├── log_staging_success / log_staging_failure
        │     └── join_staging
        │
        ├── transform_data_group
        │     ├── execute_transform_job  (dbt-transform-job Cloud Run Job)
        │     ├── log_transform_success / log_transform_failure
        │     └── join_transform
        │
        ├── test_marts_group
        │     ├── execute_marts_test_job (dbt-marts-test-job Cloud Run Job)
        │     ├── log_marts_test_success / log_marts_test_failure
        │     └── join_marts_test
        │
        ├── log_pipeline_success → send_success_email → end  (all success path)
        └── send_failure_email                         → end  (any failure path)
```

---

## Why Cloud Composer 3

| | Local Airflow (Astro CLI + ngrok) | Cloud Composer 3 |
|-|-----------------------------------|------------------|
| Availability | Only when machine is on | Always on |
| Trigger URL | Changes every ngrok session | Permanent stable HTTPS URL |
| Auth | Basic auth (admin/admin) | Google IAM token via AuthorizedSession |
| Scaling | Fixed local resources | Fully serverless — automatic |
| Cost model | Free locally | Per task execution — idle near zero |
| Infrastructure | GKE cluster (Composer 2) | Fully serverless — no nodes to manage |
| Production-ready | No | Yes |

Composer 3 is fully serverless — no GKE cluster underneath, no node pools, no
cluster upgrades. Google manages the infrastructure entirely.

---

## Critical Version Clarification

These version numbers are completely independent:

```
Composer 3    ← GCP infrastructure version
Airflow 2.x   ← the Airflow version running inside Composer 3
API v1        ← the REST API version exposed by Airflow 2.x
```

**Composer 3 runs Airflow 2.x — not Airflow 3.**
The correct REST API endpoint is `/api/v1/` not `/api/v2/`.

The local Astro CLI setup used Airflow 3.x (which exposes `/api/v2/`). These
are different environments on different Airflow versions.

Always verify before constructing API URLs:

```bash
gcloud composer environments describe stratum-composer \
  --location=asia-south2 \
  --format="value(config.softwareConfig.imageVersion)"
# composer-3.0.0-airflow-2.9.3 → use /api/v1/
# future airflow-3.x.x         → use /api/v2/
```

---

## Project Structure

```
composer/
├── dags/
│   ├── transform.py                  ← main ELT orchestration DAG
│   └── stratum_dataplex_quality.py   ← Dataplex quality + profile scan DAG (upcoming)
├── setup_composer.sh                 ← create environment + permissions (run once)
├── deploy_dags.sh                    ← upload DAGs to Composer GCS bucket
├── setup_connections.sh              ← configure SMTP connection
└── composer_requirements.txt         ← PyPI packages for Composer environment
```

---

## GCP Setup

### Step 1 — Enable APIs

```bash
gcloud services enable \
  composer.googleapis.com \
  datalineage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project=sales-datawarehouse
```

### Step 2 — Create Composer 3 environment

```bash
gcloud composer environments create stratum-composer \
  --location=asia-south2 \
  --composer-version=3 \
  --project=sales-datawarehouse
```

Composer 3 does not require `--environment-size` or `--node-count` — there
are no nodes. Creation takes 20–30 minutes.

### Step 3 — Get the Composer webserver URL

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

This base URL is used in the Cloud Run function (`COMPOSER_DAG_URL`) and for
accessing the Airflow UI in the browser. Copy-paste from the browser is the
most common source of URL errors — the CLI output is always clean.

---

## Service Account Permissions

### Cloud Run function SA (`bigquery-service@sales-datawarehouse.iam.gserviceaccount.com`)

```bash
# Call Composer REST API to trigger DAGs
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:bigquery-service@sales-datawarehouse.iam.gserviceaccount.com" \
  --role="roles/composer.user"
```

Full set of roles for the Cloud Run function SA:

| Role | Purpose | When added |
|------|---------|------------|
| `roles/storage.objectCreator` | Write files to GCS | Part 1 |
| `roles/storage.objectViewer` | Read uploaded Excel file | Part 2.5 debugging |
| `roles/bigquery.dataEditor` | Write to BigQuery | Existing |
| `roles/bigquery.jobUser` | Submit BQ jobs | Existing |
| `roles/composer.user` | Call Composer REST API | Part 2.5 debugging |

### Composer SA

Get the exact email from GCP Console → Cloud Composer → Environment Configuration
→ Service account.

```bash
COMPOSER_SA="YOUR-COMPOSER-SA@sales-datawarehouse.iam.gserviceaccount.com"

# Core Composer operations — GCS, logging, metrics, internal API
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/composer.worker"

# Trigger Cloud Run Job executions
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/run.invoker"

# Poll Cloud Run Job execution status — required for deferrable=True
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/run.viewer"

# Dataplex scan operations
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/dataplex.editor"

# Act as the Cloud Run Job SA when submitting executions
# SA-level binding — not project-level
gcloud iam service-accounts add-iam-policy-binding \
  bigquery-service@sales-datawarehouse.iam.gserviceaccount.com \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/iam.serviceAccountUser"
```

**Why `iam.serviceAccountUser` is bound on the SA not the project:**

Composer SA submits Cloud Run Job executions that run *as* `bigquery-service@...`.
GCP requires an explicit binding saying "Composer SA is allowed to act as the job
SA." Granting at project level would allow impersonation of every SA in the project
— a security risk. Binding it on the target SA is the least-privilege approach.

Without this binding:
```
PERMISSION_DENIED: Permission 'iam.serviceaccounts.actAs' denied on
service account bigquery-service@sales-datawarehouse.iam.gserviceaccount.com
```

---

## Airflow Configuration

### Environment variables

```bash
gcloud composer environments update stratum-composer \
  --location=asia-south2 \
  --update-env-variables="\
PROJECT_ID=sales-datawarehouse,\
REGION=asia-south2,\
ALERT_EMAIL=your-email@gmail.com"
```

Read in the DAG via `os.getenv()` with no fallback — fails loudly if missing:

```python
PROJECT_ID  = os.getenv('PROJECT_ID')
REGION      = os.getenv('REGION')
ALERT_EMAIL = os.getenv('ALERT_EMAIL')
```

### Airflow config overrides

```bash
gcloud composer environments update stratum-composer \
  --location=asia-south2 \
  --update-airflow-configs="\
core-dags_are_paused_at_creation=True,\
core-max_active_runs_per_dag=1,\
scheduler-min_file_process_interval=60"
```

| Config | Value | Why |
|--------|-------|-----|
| `dags_are_paused_at_creation` | `True` | Prevents accidental runs on first upload |
| `max_active_runs_per_dag` | `1` | Prevents overlapping monthly runs |
| `min_file_process_interval` | `60` | Scheduler re-parses DAGs every 60s |

### SMTP email configuration

Two parts are required — both must be set for `EmailOperator` to work.

**Part A — Airflow config overrides** (tells Airflow which backend and server):

Set in GCP Console → Cloud Composer → your environment → Airflow configuration
overrides:

```
email / email_backend     airflow.utils.email.send_email_smtp
smtp  / smtp_host         smtp.gmail.com
smtp  / smtp_starttls     True
smtp  / smtp_ssl          False
smtp  / smtp_port         587
smtp  / smtp_mail_from    your-email@gmail.com
```

**Part B — SMTP connection** (provides credentials):

```bash
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

Gmail App Password: `myaccount.google.com → Security → 2-Step Verification →
App passwords`

Both config overrides AND the connection are required. Config overrides define
the SMTP behaviour. The connection provides the password. Without the connection
Airflow knows where to send but cannot authenticate.

### Web server network access control

Set to **Allow all IP addresses**. Required because the Cloud Run function runs
on Google's infrastructure with dynamic IPs that cannot be whitelisted. Security
is enforced at the IAM layer — a valid `AuthorizedSession` token from an
authorised SA is required to trigger DAGs regardless of network access settings.

---

## The DAG — `transform.py`

### Design decisions

**TaskFlow API** — the DAG is written using Airflow 2.x decorators throughout.
`@dag`, `@task`, `@task_group` replace the verbose `with DAG() as dag:`,
`PythonOperator`, and `with TaskGroup():` patterns. Traditional operators
(`CloudRunExecuteJobOperator`, `EmailOperator`) coexist alongside decorators —
`@task` wraps Python callables only, not operator classes.

**`schedule=None`** — the DAG is event-driven only. It never runs on a schedule.
It wakes up exclusively when the Cloud Run function calls the Composer REST API
after successfully loading data to BigQuery.

**`deferrable=True`** on all three `CloudRunExecuteJobOperator` tasks — the
worker submits the Cloud Run Job and immediately frees its slot. The Airflow
Triggerer polls GCP asynchronously until the job finishes, then resumes the
worker. Without this, the worker holds a slot idle for the entire job duration.

```
deferrable=False  →  worker slot occupied ~20 minutes (all 3 jobs combined)
deferrable=True   →  worker slot occupied seconds total
```

**`_make_log_task` factory** — a factory function defined outside the DAG that
produces `@task` decorated logging functions with unique task IDs. Needed because
the same logging logic is reused across three task groups — `@task` decorated
functions used multiple times in one DAG each require a distinct `task_id`.
Duration is pulled from the upstream Cloud Run operator's task instance for
accurate job execution timing.

### Trigger rule map

| Task | Trigger rule | Reasoning |
|------|-------------|-----------|
| `execute_*_job` | `ALL_SUCCESS` (default) | Only run if upstream succeeded |
| `log_*_success` | `ALL_SUCCESS` | Only log if job succeeded |
| `log_*_failure` | `ONE_FAILED` | Log if job failed |
| `join_staging` | `ALL_DONE` | Always close staging group |
| `join_transform` | `NONE_FAILED_MIN_ONE_SUCCESS` | Propagates skips downstream |
| `join_marts_test` | `NONE_FAILED_MIN_ONE_SUCCESS` | Propagates skips downstream |
| `log_pipeline_success` | `ALL_SUCCESS` | Only on completely clean run |
| `send_success_email` | `ALL_SUCCESS` | Only on completely clean run |
| `send_failure_email` | `ONE_FAILED` | Fires if any task in the pipeline failed |
| `end` | `NONE_FAILED_MIN_ONE_SUCCESS` | Runs after either email path |

### Dependency wiring

```python
staging_group   = test_raw_data_group()
transform_group = transform_data_group()
marts_group     = test_marts_group()

# sequential main flow
start >> staging_group >> transform_group >> marts_group

# success path — all three groups must succeed
marts_group >> log_pipeline_success >> send_success_email >> end

# failure path — ONE_FAILED fires if any upstream task failed
marts_group >> send_failure_email >> end
```

---

## Deploying DAGs

DAGs live in a GCS bucket managed by Composer. Composer picks up changes within
1–3 minutes of upload — no restart needed.

```bash
# Get the bucket path
BUCKET=$(gcloud composer environments describe stratum-composer \
  --location=asia-south2 \
  --format="value(config.dagGcsPrefix)")

# Upload DAGs
gcloud storage cp dags/transform.py                $BUCKET/
gcloud storage cp dags/stratum_dataplex_quality.py $BUCKET/
```

New DAGs are paused by default. Unpause before triggering:

```bash
# via CLI
gcloud composer environments run stratum-composer \
  --location=asia-south2 \
  dags unpause -- transformation_pipeline

# or toggle the pause switch in the Airflow UI
```

---

## Triggering the DAG

### Via Cloud Run function (primary — event-driven)

When a monthly Excel file is uploaded through the FastAPI UI, Eventarc fires
the Cloud Run extract-load function. After loading data to BigQuery it calls:

```
POST https://YOUR-COMPOSER-URL/api/v1/dags/transformation_pipeline/dagRuns
Authorization: Bearer <Google ID token via AuthorizedSession>

{
  "logical_date": "2026-05-17T10:00:00Z",
  "conf": {
    "filename": "Sales Data.xlsx",
    "bucket": "sales-dw-bucket"
  }
}
```

### Via CLI (manual testing)

```bash
gcloud composer environments run stratum-composer \
  --location=asia-south2 \
  dags trigger -- transformation_pipeline
```

### Monitor DAG runs

```bash
# list recent runs
gcloud composer environments run stratum-composer \
  --location=asia-south2 \
  dags list-runs -- -d transformation_pipeline
```

Or via Airflow UI: open the environment → DAGs → transformation_pipeline →
Grid view → click any task → Logs.

---

## Known Issues and Lessons Learned

### Issue 1 — Deferrable operator deferred instead of skipping

**Symptom:** Staging failed. Transform group was correctly skipped. Marts test
group deferred instead of skipping.

**Root cause:** `branch_after_staging` explicitly skipped `transform_data_group`
— Airflow marked it skipped before the deferrable operator could start. But
`test_marts_group` had no explicit skip instruction. Its join (`ALL_DONE`) ran
and completed, so Airflow handed `execute_marts_test_job` to the Triggerer before
skip logic could intervene.

**Fix:** Changed `join_transform` and `join_marts_test` from `ALL_DONE` to
`NONE_FAILED_MIN_ONE_SUCCESS`. This correctly propagates skips when upstream
was skipped — deferrable operators are not handed to the Triggerer unless the
join actually succeeded.

### Issue 2 — Join operator masked failure, sent wrong email

**Symptom:** `execute_marts_test_job` failed. `join_marts_test` with `ALL_DONE`
ran and reported success. `send_failure_email` with `ONE_FAILED` saw a succeeded
join upstream and was skipped. Success email was sent instead.

**Root cause:** `ALL_DONE` always runs and always reports success regardless of
upstream failure. The join masked the failure from `send_failure_email`.

**Fix:** Same fix as Issue 1 — `NONE_FAILED_MIN_ONE_SUCCESS` on the join
correctly fails when its upstream failed, allowing `ONE_FAILED` on
`send_failure_email` to fire.

### Issue 3 — Duplicate rows in `fct_grocery_sales`

**Symptom:** All 364 `sale_key` values had exactly 2 copies after the first
Airflow-triggered pipeline run.

**Root cause:** The fact table was already populated from manual Cloud Run job
test executions. The incremental `MERGE` statement processed the same data again
against already-loaded rows, resulting in duplicates.

**Fix:**
```sql
-- Step 1: deduplicate immediately
CREATE OR REPLACE TABLE `sales-datawarehouse.warehouse.fct_grocery_sales` AS
SELECT DISTINCT * FROM `sales-datawarehouse.warehouse.fct_grocery_sales`;

-- Step 2: verify
SELECT sale_key, COUNT(*) AS cnt
FROM `sales-datawarehouse.warehouse.fct_grocery_sales`
GROUP BY sale_key HAVING COUNT(*) > 1;
```

Then do a dbt full refresh to reset incremental state — temporarily add
`--full-refresh` to `Dockerfile.transform` CMD, rebuild, execute once, revert.

**Prevention:** Never mix manual Cloud Run job executions with Airflow-triggered
runs on the same data without verifying target tables are clean first.

### Issue 4 — Branch showed as skipped despite correct log output

**Symptom:** Branch log showed "Branch into send_failure_email" but the branch
task itself showed as skipped (pink) in the Airflow UI. `send_failure_email`
was also skipped.

**Root cause:** The log shown was from a previous run. In the current run the
branch was being skipped before execution because its upstream (`join_marts_test`
with `ALL_DONE`) was completing and the skipped state was not propagating through
the task group boundary to the branch correctly.

**Fix:** Same — `NONE_FAILED_MIN_ONE_SUCCESS` on joins resolved the propagation
issue, allowing the branch to evaluate and route correctly.

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `403` on DAG trigger | SA missing `composer.user` | Add role to Cloud Run SA |
| `404` on DAG trigger | Wrong API version or DAG ID | Use `/api/v1/` for Airflow 2.x |
| DNS resolution failure | Malformed Composer URL | Use CLI to get URL, not browser |
| DAG paused after upload | `dags_are_paused_at_creation=True` | Unpause via UI or CLI |
| Email not sending | Config overrides OR connection missing | Set both — config + smtp_default connection |
| Deferrable task defers on skip path | `ALL_DONE` join not propagating skips | Use `NONE_FAILED_MIN_ONE_SUCCESS` on joins |
| Wrong email sent after failure | Join masking failure state | Use `NONE_FAILED_MIN_ONE_SUCCESS` on joins |
| Duplicate fact table rows | Mixed manual + DAG runs on same data | Deduplicate BQ table + dbt full refresh |
| `actAs` permission denied | Composer SA missing SA-level binding | Add `iam.serviceAccountUser` on job SA |
| App Password not working | Wrong account or 2FA not enabled | Generate App Password from correct Google account |

---

## Next Step

With Cloud Composer fully operational and the pipeline running end to end —
file upload → Cloud Run extract-load → Composer DAG → 3 dbt Cloud Run Jobs →
email alerts — the final part is configuring **Dataplex Universal Catalog**
for automatic data lineage, metadata discovery, and ongoing quality governance.
The `stratum_dataplex_quality.py` DAG is already written and ready — it activates
once the `TriggerDagRunOperator` is uncommented in `transform.py`.
