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
