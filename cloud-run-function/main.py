import functions_framework

import requests
import pandas as pd
import logging
import sys

from io import BytesIO
from pathlib import Path

from datetime import datetime, UTC

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
import google.auth
from google.auth.transport.requests import AuthorizedSession

# ── Config ───────────────────────────────────────────────────────────────────
TABLE_ID = "sales-datawarehouse.raw_data.sales"
WEB_SERVER_URL = "https://88d2e9eba36448fbad243b7923dfbd8b-dot-asia-south2.composer.googleusercontent.com"

# ── Setup Logging correctly for Cloud Run ────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,                        # Forces logger to capture INFO statements
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout                          # Redirects logs to stdout so Cloud Run intercepts them
)
logger = logging.getLogger(__name__)

# 1. Initialize credentials at startup (Google Best Practice)
# The "cloud-platform" scope allows AuthorizedSession to pass the identity via IAM
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
DAG_ID = 'transformation_pipeline'

def _trigger_composer_dag(filename: str, bucket: str, web_server_url: str, dag_id: str):
    """
    Triggers an Airflow 3 DAG using Google's AuthorizedSession wrapper.
    Safely captures network exceptions to prevent Cloud Run infinite retry loops.
    """
    # Build the correct Airflow REST API v2 endpoint
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url.rstrip('/')}/{endpoint}"

    # Build Airflow 3 compatible payload
    payload = {
        "logical_date": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conf": {
            "filename": filename,
            "bucket":   bucket,
        },
    }

    try:
        # 2. Open the authenticated session
        authed_session = AuthorizedSession(CREDENTIALS)
        
        # 3. Post the trigger with a defensive timeout
        # If your function was hanging before, this prevents it from freezing up
        response = authed_session.request(
            method="POST",
            url=request_url,
            json=payload,
            timeout=30  
        )
        
        # 4. Catch Airflow-specific RBAC Authorization failures
        if response.status_code == 403:
            logger.error(
                "IAM authentication succeeded, but Airflow RBAC denied the operation. "
                f"Ensure the Service Account is registered inside Airflow with 'Op' or 'User' rights. "
                f"Details: {response.text}"
            )
            return None
            
        # Catch other status anomalies (400, 404, 500)
        response.raise_for_status()
        
        dag_run_id = response.json().get("dag_run_id")
        logger.info(f"DAG triggered successfully — ID: {dag_run_id}")
        return dag_run_id

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error running Composer task: {http_err} | Details: {response.text if response else ''}")
        return None
    except requests.exceptions.Timeout:
        logger.error("The network connection to Cloud Composer webserver timed out after 30s.")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in pipeline trigger: {str(e)}")
        return None


@functions_framework.cloud_event
def extract_and_load(cloud_event):
    data = cloud_event.data
    filename = data["name"]
    bucket = data["bucket"]

    if not filename.endswith((".xlsx", ".xls")):
        logger.info(f"Skipped non-Excel file: {filename}")
        return "Skipped non-Excel file", 200

    # Wrapper to catch ANY processing anomaly and prevent Eventarc loop crashes
    try:
        # ── Extract ───────────────────────────────────────────────────────────
        logger.info(f"Downloading {filename} from bucket {bucket}")
        blob = storage.Client().bucket(bucket).blob(filename)
        
        # Note: Ensure Cloud Run memory is set to >= 1GB/2GB for this engine
        df = pd.read_excel(BytesIO(blob.download_as_bytes()))

        # ── Clean & Normalize ─────────────────────────────────────────────────
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r'[ /]+', '_', regex=True)
            .str.replace(r'[^0-9a-zA-Z_]', '', regex=True)
        )

        BATCH_ID = datetime.now(UTC).strftime("batch_%Y_%m")
        df['source_file_name'] = Path(filename).name
        df['batch_id'] = BATCH_ID
        df['loaded_at'] = datetime.now(UTC)

        # CRITICAL FIX: Convert Pandas Datetime64 objects to clean Python dates
        # to match your BigQuery 'DATE' schema targets perfectly.
        for col in ['order_date', 'shipped_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date

        # ── Dedup check ───────────────────────────────────────────────────────
        bq_client = bigquery.Client()
        try:
            # Using parameterized query or clear string formatting for tracking
            query = f"SELECT COUNT(1) AS count FROM `{TABLE_ID}` WHERE batch_id = '{BATCH_ID}'"
            result = bq_client.query(query).result()
            
            if list(result)[0].count > 0:
                logger.info(f"Batch {BATCH_ID} already loaded — skipping BQ load, triggering DAG anyway.")
                _trigger_composer_dag(filename, bucket, WEB_SERVER_URL, DAG_ID)
                logger.info(f"Duplicate batch bypassed, DAG fired")
                return "Duplicate batch bypassed, DAG fired", 200
        except NotFound:
            logger.info("Target table not found — BigQuery will create it on load execution.")

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

        logger.info(f"Streaming dataframe load to BigQuery table: {TABLE_ID}")
        job = bq_client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()  # Waits for the job to complete

        logger.info(f"BigQuery write pipeline finished successfully.")

        # ── Trigger Composer DAG ──────────────────────────────────────────────
        dag_run_id = _trigger_composer_dag(filename, bucket, WEB_SERVER_URL, DAG_ID)
        
        if dag_run_id is None:
            logger.error("Data was loaded into BQ, but Cloud Composer DAG failed to trigger.")
        
        return "Processing complete", 200

    except Exception as err:
        # Crucial fallback: Log full exception detail context for Cloud Logging
        logger.critical(f"Pipeline crashed during execution: {str(err)}", exc_info=True)
        
        # Return 200 to acknowledge receipt so Eventarc ceases destructive retries
        return "Internal execution failure intercepted safely", 200