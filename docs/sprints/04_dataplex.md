# Part 4 — Dataplex Universal Catalog Data Quality & Profile Scans

## Overview

This document covers the final part of the Stratum ELT pipeline — automated
data quality scanning and column profiling using **Dataplex Universal Catalog**
(formerly known as Dataplex, rebranded by Google in April 2026).

After the three dbt Cloud Run Jobs complete successfully in the
`transformation_pipeline` DAG, the pipeline chains to a second DAG
(`automated_data_quality_check_and_profile_scan_pipeline`) which runs quality
scans across every mart model and a column profile scan on the central fact
table. Scan results are exported to BigQuery and a summary email is sent.

This is **Part 4** — the final part of the full serverless migration:

| Part | Component | Status |
|------|-----------|--------|
| 1 | FastAPI upload server → Cloud Run service | ✅ Complete |
| 2 | dbt transformations → Cloud Run Jobs (x3) | ✅ Complete |
| 2.5 | Extract-load function → Cloud Run service | ✅ Complete |
| 3 | Airflow DAG → Cloud Composer 3 | ✅ Complete |
| **4** | **Dataplex Universal Catalog scans** | ✅ Complete |

---

## Naming Note

Google rebranded Dataplex to **Dataplex Universal Catalog** in April 2026. The
Airflow provider operator names, GCP API endpoints, and `gcloud dataplex` CLI
commands are all unchanged. The UI calls it Dataplex Universal Catalog but
everything in code is identical to pre-rebrand.

---

## How Dataplex Fits Into the Pipeline

```
transformation_pipeline DAG
        │
        ├── test_raw_data_group      (dbt-staging-job)
        ├── transform_data_group     (dbt-transform-job)
        ├── test_marts_group         (dbt-marts-test-job)
        ├── log_pipeline_success
        ├── send_success_email
        │
        └── TriggerDagRunOperator
                │  trigger_dag_id = automated_data_quality_check_and_profile_scan_pipeline
                │  wait_for_completion=True, deferrable=True
                ▼
automated_data_quality_check_and_profile_scan_pipeline DAG
        │
        ├── fct_quality_scan_group      → fct_grocery_sales structural integrity
        │       ↓
        ├── revenue_marts_scan_group    → monthly + quarterly + yearly revenue
        │       ↓
        ├── growth_models_scan_group    → ytd + mtd + qtd growth models
        │       ↓
        ├── rankings_scan_group         → all 5 ranking models
        │       ↓
        ├── shipment_scan_group         → shipment expenditure model
        │
        ├── profile_scan_group          → fct_grocery_sales column statistics
        │   (runs in parallel with quality groups)
        │
        └── log_pipeline_success → send_summary_email → end
```

The `TriggerDagRunOperator` in `transform.py` uses `deferrable=True` and
`wait_for_completion=True` — the ELT pipeline waits for all scans to finish
before marking itself complete, without holding a worker slot during the wait.

---

## Dataplex vs dbt Tests — Why Both

A question worth addressing directly — why run Dataplex scans when dbt already
has tests?

| | dbt tests | Dataplex scans |
|-|-----------|---------------|
| When they run | During `dbt test` inside the Cloud Run Job | After transformation, independently |
| Managed by | dbt project code | GCP-managed scan definitions |
| Results stored | dbt logs, Cloud Run logs | BigQuery `warehouse.dq_results` — queryable |
| Visible in GCP Console | No | Yes — Dataplex Universal Catalog UI |
| Data lineage | No | Yes — BigQuery lineage integration |
| Profile statistics | No | Yes — null counts, min/max/mean, distributions |
| Suitable for BI governance | No | Yes |

dbt tests are developer-facing — they catch issues during transformation and
stop the pipeline. Dataplex scans are governance-facing — they provide an
auditable, queryable quality record for every monthly run visible in the GCP
Console. Together they form a two-layer quality framework.

---

## GCP Setup

### Step 1 — Enable the Dataplex API

```bash
gcloud services enable dataplex.googleapis.com \
  --project=sales-datawarehouse
```

### Step 2 — Grant Dataplex SA permissions to scan BigQuery

When the Dataplex API is enabled, GCP automatically creates a managed SA:

```bash
PROJECT_NUMBER=$(gcloud projects describe sales-datawarehouse \
  --format="value(projectNumber)")

DATAPLEX_SA="service-${PROJECT_NUMBER}@gcp-sa-dataplex.iam.gserviceaccount.com"
```

This SA runs the actual scan jobs inside BigQuery. It needs three roles:

```bash
# Read the warehouse tables being scanned
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${DATAPLEX_SA}" \
  --role="roles/bigquery.dataViewer"

# Submit BigQuery jobs that power the scan execution
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${DATAPLEX_SA}" \
  --role="roles/bigquery.jobUser"

# Write scan results to warehouse.dq_results
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${DATAPLEX_SA}" \
  --role="roles/bigquery.dataEditor"
```

### Step 3 — Grant Composer SA permission to manage scans

The Composer SA needs `roles/dataplex.editor` to create, update, and run scans
via the Airflow operators:

```bash
COMPOSER_SA="YOUR-COMPOSER-SA@sales-datawarehouse.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/dataplex.editor"
```

### Step 4 — Do NOT pre-create `warehouse.dq_results`

Dataplex owns the schema of the export table. If you create it manually with
any schema — even a partial one — Dataplex rejects it:

```
The table is not a Standard BigQuery table or its schema
doesn't match the required export table schema
```

Leave the table non-existent. Dataplex creates it automatically with the correct
schema on the first successful scan run. If you accidentally created it, delete
it first:

```bash
bq rm -f sales-datawarehouse:warehouse.dq_results
```

**This is a hard rule for any GCP-managed export table** — never pre-create
tables that a managed service owns the schema for.

---

## Permission Architecture

Two separate service accounts with separate responsibilities:

```
Composer SA
    │  creates/updates/triggers scans via Dataplex API
    │  roles/dataplex.editor
    ▼
Dataplex API
    │  spins up scan execution jobs
    ▼
Dataplex managed SA (service-NNNN@gcp-sa-dataplex.iam.gserviceaccount.com)
    │  reads warehouse tables, submits BQ jobs, writes dq_results
    │  roles/bigquery.dataViewer
    │  roles/bigquery.jobUser
    │  roles/bigquery.dataEditor
    ▼
BigQuery warehouse dataset
```

The Composer SA orchestrates via the Dataplex API. The Dataplex SA does the
actual work inside BigQuery. They are separate identities — granting BQ roles
to the Composer SA would not fix a Dataplex execution failure.

---

## Scan Coverage

### Quality scans — 6 scan groups covering all mart models

| Scan group | Tables scanned | Rules |
|------------|---------------|-------|
| `fct_quality_scan_group` | `fct_grocery_sales` | PK not null + unique, 8 FK completeness, 5 financial validity, 2 audit completeness |
| `revenue_marts_scan_group` | `monthly_revenue_growth`, `quarterly_revenue_growth`, `yearly_revenue_growth` | Year/month/quarter completeness, revenue validity, growth rate range |
| `growth_models_scan_group` | `ytd_revenue_qty_shipment_growth`, `mtd_revenue_qty_shipment_growth`, `qtd_revenue_qty_shipment_growth` | Date completeness + uniqueness, daily revenue validity, cumulative revenue completeness + validity |
| `rankings_scan_group` | 5 ranking models (city, customer, product, region, salesperson) | Dimension column completeness + uniqueness, revenue validity, rank completeness + validity |
| `shipment_scan_group` | `shipment_exp_comparison_by_ship_company` | Shipper uniqueness, fees + shipments validity, cost percent range (threshold 0.95 for NULLIF) |
| `profile_scan_group` | `fct_grocery_sales` | Full default column profile — min, max, mean, null counts, distinct counts, distributions |

**Tables deliberately excluded from scanning:**

`dim_date` — static date spine, never changes between monthly loads, no quality
scan needed.

`dim_*` dimension tables — validated by dbt referential integrity tests during
Job 3. Dataplex scanning them would be redundant.

### Rule dimensions used

| Dimension | What it checks |
|-----------|---------------|
| `COMPLETENESS` | Column must not be null (threshold 1.0 = 100% of rows) |
| `UNIQUENESS` | Column values must be distinct across all rows |
| `VALIDITY` | Column values must fall within a specified range |

### Threshold notes

- `threshold: 1.0` — 100% of rows must pass. Used for structural columns
  (PKs, FKs, measures) where any failure is a data issue.
- `threshold: 0.95` — 95% of rows must pass. Used for `mom_growth_rate`,
  `qoq_growth_rate`, `yoy_growth_rate` (first row has a null LAG value by
  design — expected) and `shipping_cost_percent_of_revenue` (NULLIF produces
  null when revenue is zero — legitimate).

---

## DAG Design

### Pattern — TaskFlow API with factory functions

The DAG uses the same TaskFlow API pattern as `transformation_pipeline`. Two
factory functions generate tasks dynamically to avoid duplicating logic across
similar scan groups:

**`_growth_rules(prefix, cumulative_col)`** — produces identical rule sets for
ytd, mtd, and qtd scans. The prefix differentiates rule names; the cumulative
column name varies per model.

**`_ranking_rules(prefix, dim_col)`** — produces identical rule sets for all
5 ranking models. The dimension column name (city, customer_name, product_name,
region, sales_person) varies per model.

**`_base_scan_body(table, rules)`** — helper that builds the full scan body
dict including the BigQuery resource path and the `post_scan_actions` export
to `warehouse.dq_results`. Keeps scan operator bodies DRY.

### Dependency structure

```python
# quality groups run sequentially — gentler on Composer worker resources
start >> fct_group >> revenue_group >> growth_group >> rankings_group >> shipment_group >> log_pipeline

# profile scan runs in parallel with quality groups — independent
start >> profile_group >> log_pipeline

log_pipeline >> send_summary_email >> end
```

Quality groups run sequentially. Profile scan runs in parallel since it is
completely independent of the quality scan definitions. Both paths converge
at `log_pipeline_success`.

### `max_active_tasks=6`

```python
@dag(
    dag_id='automated_data_quality_check_and_profile_scan_pipeline',
    max_active_tasks=6,
    ...
)
```

Limits concurrent tasks across the entire DAG to 6 at any one time. Prevents
Composer worker pod exhaustion when multiple scan groups queue simultaneously.

### Revenue group — serialised creates, parallel runs

```python
# create operators run sequentially — stays within Dataplex API rate limit
create_monthly >> create_quarterly >> create_yearly

# run operators execute in parallel — Dataplex handles the heavy lifting
create_monthly   >> run_monthly   >> get_monthly
create_quarterly >> run_quarterly >> get_quarterly
create_yearly    >> run_yearly    >> get_yearly
```

The create/update operators call the Dataplex API to register scan definitions.
Running them sequentially respects the 10 requests-per-minute quota. The run
operators submit the actual scan jobs to Dataplex — these are handed off to
GCP's serverless Spark infrastructure and run in parallel without consuming
Composer worker slots.

This serialise-creates pattern could not be applied to the growth and rankings
groups because they use a `for` loop factory pattern — the `max_active_tasks=6`
DAG-level limit handles rate limiting for those groups instead.

### `deferrable=True` on all Run operators

All `DataplexRunDataQualityScanOperator` and `DataplexRunDataProfileScanOperator`
instances use `deferrable=True`:

```python
run = DataplexRunDataQualityScanOperator(
    task_id='run_fct_quality_scan',
    project_id=PROJECT_ID,
    region=REGION,
    data_scan_id='fct-quality-scan',
    deferrable=True,    # ← hands off to Triggerer while scan runs on GCP
)
```

**Why this is critical for Dataplex operators specifically:**

Dataplex scans run on a serverless Spark backend. A single scan can take 5–10
minutes. Without `deferrable=True`, the operator sits in a tight Python
`time.sleep` polling loop, holding a worker thread open with a long-lived HTTPS
socket. GKE's ingress infrastructure flags this as a dead socket and drops it.
Simultaneously, the Airflow scheduler detects the worker is not emitting
heartbeats and marks the task as failed — while the task is still technically
running on the worker. This produces a **Scheduler-Executor Mismatch (Zombie
Task)** — the scheduler says failed, the worker says running, logs never appear.

With `deferrable=True` the worker submits the scan job and immediately suspends,
freeing the thread. The Airflow Triggerer uses Python `asyncio` to watch the job
status asynchronously — no blocking socket, no heartbeat gap, no zombie.

**Important:** only `DataplexRunDataQualityScanOperator` and
`DataplexRunDataProfileScanOperator` support `deferrable=True`. The
Create/Update and Get operators do not accept this parameter and will raise:

```
AirflowException: Invalid arguments were passed to
DataplexCreateOrUpdateDataQualityScanOperator.
**kwargs: {'deferrable': True}
```

---

## Integration with `transformation_pipeline`

The ELT DAG triggers the quality DAG via `TriggerDagRunOperator`:

```python
trigger_dataplex_dag = TriggerDagRunOperator(
    task_id='trigger_dataplex_quality_dag',
    trigger_dag_id='automated_data_quality_check_and_profile_scan_pipeline',
    wait_for_completion=True,
    deferrable=True,
    failed_states=['failed'],
    trigger_rule=TriggerRule.NONE_FAILED,
)
```

`trigger_rule=TriggerRule.NONE_FAILED` — the quality DAG is triggered whether
the ELT succeeded or the failure email fired, as long as nothing raised an
unexpected exception. This ensures scans run even when some mart tests have
warnings, giving you a quality report regardless.

`wait_for_completion=True` combined with `deferrable=True` — the ELT DAG
waits for all scans to finish before marking itself complete, but does so
asynchronously via the Triggerer. The ELT run in Composer shows as running
until the entire quality pipeline finishes.

`failed_states=['failed']` — if the quality DAG itself fails (not just
individual scan rule failures, but an actual DAG failure), the ELT DAG marks
the trigger task as failed.

---

## Scan Results in BigQuery

All quality scan results are exported to `warehouse.dq_results` automatically
by the `post_scan_actions.bigquery_export` configuration in each scan body.
Dataplex creates and manages this table — never create it manually.

The table contains one row per rule per scan execution with columns including:
- `data_scan_id` — which scan produced the result
- `job_id` — the specific scan execution
- `rule_name` — the rule that was evaluated
- `passed` — boolean pass/fail
- `pass_ratio` — percentage of rows that passed
- `evaluated_count` — total rows evaluated
- `passed_count` — rows that passed

This makes `warehouse.dq_results` queryable directly in BigQuery for trend
analysis across monthly runs:

```sql
-- See all rules that failed in the latest scan run
SELECT
    data_scan_id,
    rule_name,
    passed,
    pass_ratio,
    evaluated_count,
    passed_count
FROM `sales-datawarehouse.warehouse.dq_results`
WHERE passed = FALSE
ORDER BY data_scan_id
```

---

## Errors Encountered and Fixes

### Error 1 — `_TaskDecorator` object has no attribute `roots`

**Symptom:** DAG broken completely on upload. Parse error in Airflow.

**Cause:** In the dependency chain, `log_pipeline_success` (a `@task` decorated
function) was placed directly into `>>` without being invoked with `()`. Airflow
received a raw `_TaskDecorator` function object instead of an instantiated task
node and failed trying to traverse its graph roots.

**Fix:** Always invoke `@task` and `@task_group` functions with `()` when
placing them in dependency chains:

```python
# wrong
marts_group >> log_pipeline_success >> send_summary_email

# correct
log_pipeline = log_pipeline_success()
marts_group >> log_pipeline >> send_summary_email
```

### Error 2 — Zombie tasks (9-minute hang, no logs, scheduler-executor mismatch)

**Symptom:** Scan tasks hung for exactly 9 minutes then failed. No logs
appeared in the Airflow UI — the log viewer showed the generic "logs not found"
message citing possible pod eviction. Scheduler logs showed tasks reported as
failed while their state attribute still showed `queued`.

**Root cause:** Dataplex operators use blocking synchronous polling by default.
The worker thread held an open HTTPS socket to the Dataplex backend while
sleeping between polls. GKE's ingress infrastructure detected the socket as
dead and dropped it. The Airflow scheduler, not receiving worker heartbeats
during the blocking sleep, marked the task as failed in the database — while
the worker still considered it running. This is a **Zombie Task** — a
scheduler-executor state mismatch.

**Fix:** `deferrable=True` on all Run operators. The Triggerer component uses
`asyncio` for non-blocking polling — no persistent socket, no heartbeat gap.

### Error 3 — Worker pod evictions from parallel task overload

**Symptom:** Scheduler logs showed repeated entries:
```
Executor reported task finished with state failed,
but the task instance's state attribute is queued
```
Tasks were failing before they even started executing.

**Cause:** All 6 scan groups starting simultaneously caused Composer to queue
18+ tasks at once. Kubernetes could not provision enough worker pods fast enough
and began evicting them before tasks ran.

**Fix 1:** `max_active_tasks=6` on the DAG — caps concurrent tasks at 6.

**Fix 2:** Sequential dependency chain for quality groups — instead of all 6
groups running in parallel, quality groups run one after another. Peak
concurrent tasks dropped from 18+ to 3 (the parallel runs inside revenue_group).

### Error 4 — `deferrable=True` accepted by wrong operator type

**Symptom:** DAG broke with:
```
AirflowException: Invalid arguments were passed to
DataplexCreateOrUpdateDataQualityScanOperator.
**kwargs: {'deferrable': True}
```

**Cause:** `deferrable=True` was accidentally added to `Create/Update` operators
instead of only `Run` operators.

**Fix:** Remove `deferrable=True` from all `Create/Update` and `Get` operators.
Only `Run` operators support deferrable mode.

Operators that support `deferrable=True`:
- `DataplexRunDataQualityScanOperator` ✅
- `DataplexRunDataProfileScanOperator` ✅

Operators that do NOT support `deferrable=True`:
- `DataplexCreateOrUpdateDataQualityScanOperator` ❌
- `DataplexCreateOrUpdateDataProfileScanOperator` ❌
- `DataplexGetDataQualityScanResultOperator` ❌
- `DataplexGetDataProfileScanResultOperator` ❌

### Error 5 — `dq_results` schema mismatch

**Symptom:**
```
The table dq_results is not a Standard BigQuery table
or its schema doesn't match the required export table schema
```

**Cause:** The `dq_results` table was manually pre-created with a custom schema.
Dataplex rejected it because its internal export schema does not match any
user-defined schema.

**Fix:** Delete the table and let Dataplex create it automatically:

```bash
bq rm -f sales-datawarehouse:warehouse.dq_results
```

Never manually create export tables owned by managed GCP services.

### Error 6 — Task group instantiation side effect

**Symptom:** Commenting out scan groups from the dependency arrays to isolate
a failing group had no effect — the commented-out groups were still being
queued and executed.

**Cause:** In the TaskFlow API, calling `group = scan_group_function()` at the
top level of a DAG registers the task group into Airflow's in-memory execution
graph immediately as a side effect of instantiation — regardless of whether it
appears in the `>>` dependency chain. Airflow treated the orphaned groups as
root nodes and scheduled them immediately.

**Fix:** Comment out both the instantiation and the dependency wiring:

```python
# wrong — group still registers even without >> wiring
# fct_group = fct_quality_scan_group()   ← still registers
revenue_group = revenue_marts_scan_group()
start >> revenue_group  # fct_group absent from chain but still scheduled

# correct — comment out instantiation entirely
# fct_group = fct_quality_scan_group()
revenue_group = revenue_marts_scan_group()
start >> revenue_group
```

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `_TaskDecorator has no attribute 'roots'` | `@task` function not invoked with `()` | Add `()` to all task/task_group calls in dependency chain |
| Zombie tasks — no logs, 9 min hang | Blocking poll holds worker socket | Add `deferrable=True` to all Run operators |
| Tasks fail in queued state | Worker pod eviction from parallel overload | Add `max_active_tasks=6`, switch to sequential dependencies |
| `Invalid arguments: deferrable=True` | `deferrable` on wrong operator type | Only on Run operators — not Create/Update or Get |
| `dq_results schema mismatch` | Table pre-created with wrong schema | Delete table, let Dataplex create it on first run |
| Groups still run when commented from chain | TaskGroup instantiation is a side effect | Comment out both instantiation variable AND chain entry |
| Dataplex API permission denied | Dataplex SA missing BQ roles | Grant `dataViewer`, `jobUser`, `dataEditor` to Dataplex SA |
| Composer SA can't create scans | Missing `dataplex.editor` | Grant role to Composer SA |

---

## Complete Project Summary

The Stratum pipeline is now fully serverless and end-to-end automated on GCP.
The full journey from a locally managed Airflow project to a cloud-native
event-driven architecture:

```
Browser
  │  drag-and-drop Excel upload
  ▼
FastAPI (Cloud Run service)          Part 1
  │  stream to GCS
  ▼
GCS bucket
  │  OBJECT_FINALIZE → Eventarc
  ▼
Cloud Run function (extract-load)    Part 2.5
  │  read xlsx → clean → load BigQuery raw_data.sales
  │  POST /api/v1/dags/transformation_pipeline/dagRuns
  ▼
Cloud Composer 3 (Airflow 2.x)       Part 3
  transformation_pipeline DAG
  │
  ├── dbt-staging-job (Cloud Run Job)     Part 2
  │     source freshness → source tests → quarantine → staging → staging tests
  │
  ├── dbt-transform-job (Cloud Run Job)   Part 2
  │     dbt run --select marts
  │
  ├── dbt-marts-test-job (Cloud Run Job)  Part 2
  │     dbt test --select marts
  │
  ├── send_success_email / send_failure_email
  │
  └── TriggerDagRunOperator
        ▼
  automated_data_quality_check_and_profile_scan_pipeline DAG    Part 4
        │
        ├── Quality scans on all mart models
        │     fct → revenue → growth → rankings → shipment
        │
        ├── Profile scan on fct_grocery_sales (parallel)
        │
        └── send_summary_email → warehouse.dq_results in BigQuery
```

**Technology stack:**

| Layer | Technology |
|-------|-----------|
| Upload UI | FastAPI + HTML/JS |
| File storage | Google Cloud Storage |
| Event trigger | Eventarc (Cloud Storage trigger) |
| Extract-Load | Cloud Run service (Python, pandas, BigQuery client) |
| Orchestration | Cloud Composer 3 (Airflow 2.x) |
| Transformation | dbt Core 1.8 + dbt-bigquery |
| Job execution | Cloud Run Jobs (3 separate jobs) |
| Data warehouse | BigQuery (raw_data → staging → warehouse) |
| Data quality | Dataplex Universal Catalog |
| Alerting | Gmail SMTP via Airflow EmailOperator |
