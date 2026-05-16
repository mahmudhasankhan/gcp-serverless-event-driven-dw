"""
───────────────────────
Main ELT orchestration DAG for the Stratum project.
Written using the TaskFlow API pattern (Airflow 2.x decorators).

Triggered externally by the Cloud Run extract-load function via the
Composer REST API when a monthly Excel file lands in GCS.

Pipeline sequence:
    test_raw_data_group     → dbt-staging-job
    branch_after_staging    → stop if staging failed
    transform_data_group    → dbt-transform-job
    test_marts_group        → dbt-marts-test-job
    send_success_email      → notify on success
    trigger_dataplex_dag    → chain to quality scans

Failure at staging       → send_failure_email → end
Failure at marts test    → send_failure_email → end
"""

from __future__ import annotations

import logging
import os

from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID  = os.environ.get('PROJECT_ID',  'sales-datawarehouse')
REGION      = os.environ.get('REGION',      'asia-south2')
ALERT_EMAIL = os.environ.get('ALERT_EMAIL', 'your-email@gmail.com')

logger = logging.getLogger(__name__)

default_args = {
    'owner':            'data-team',
    'depends_on_past':  False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,   # handled explicitly via EmailOperator
    'email_on_retry':   False,
}


# ── Reusable task: log job status ─────────────────────────────────────────────
# Defined outside the DAG so it can be reused across all three task groups
# without duplication. TaskFlow @task functions are reusable like regular Python.

def _make_log_task(task_id: str, job_name: str, status: str, trigger_rule: TriggerRule):
    """
    Factory that produces a @task decorated logging function with a unique task_id.
    Needed because @task decorated functions used multiple times in one DAG
    each need a distinct task_id.
    """
    def _make_log_task(task_id: str, upstream_task_id: str, job_name: str, status: str, trigger_rule: TriggerRule):
    """
    Factory that produces a @task decorated logging function with a unique task_id.
    Pulls duration metrics dynamically from the specified upstream task.
    """
    @task(task_id=task_id, trigger_rule=trigger_rule)
    def log_job_status(**context):
        ti = context['ti']
        dag_run = context['dag_run']
        
        # Fetch the task instance of the actual Cloud Run job
        upstream_ti = dag_run.get_task_instance(upstream_task_id)
        
        # Safely extract duration, fallback to 0 if it somehow hasn't recorded
        duration = upstream_ti.duration if upstream_ti and upstream_ti.duration else 0
        duration_rounded = round(duration, 2)

        logger.info(
            "\n"
            "══════════════════════════════════════════\n"
            f"Cloud Run Job Status : {status}\n"
            "══════════════════════════════════════════\n"
            f"Job Name       : {job_name}\n"
            f"Logger Task ID : {ti.task_id}\n"
            f"Target Task ID : {upstream_task_id}\n"
            f"Execution Date : {context['ds']}\n"
            f"Duration       : {duration_rounded}s\n"
            "══════════════════════════════════════════"
        )
    return log_job_status


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id='transformation_pipeline',
    default_args=default_args,
    description='event-driven ELT — triggered by Cloud Run on GCS file upload',
    start_date=datetime(2026, 1, 1),
    schedule=None,         # triggered externally via Composer REST API
    catchup=False,
    tags=['elt', 'dbt', 'cloud-run'],
)
def transform():

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(
    task_id='end', 
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

    # ── Task Group 1: Staging ──────────────────────────────────────────────────
    # dbt-staging-job runs:
    #   dbt source freshness → source tests → quarantine → staging run → staging tests

    @task_group(group_id='test_raw_data_group')
    def test_raw_data_group():

        execute_staging_job = CloudRunExecuteJobOperator(
            task_id='execute_staging_job',
            project_id=PROJECT_ID,
            region=REGION,
            job_name='dbt-staging-job',
            deferrable=True,
        )

        log_success = _make_log_task(
            task_id='log_staging_success',
            job_name='dbt-staging-job',
            status='SUCCESS ✅',
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )()

        log_failure = _make_log_task(
            task_id='log_staging_failure',
            job_name='dbt-staging-job',
            status='FAILED ❌',
            trigger_rule=TriggerRule.ONE_FAILED,
        )()

        join = EmptyOperator(
            task_id='join_staging',
            trigger_rule=TriggerRule.ALL_DONE,
        )

        execute_staging_job >> [log_success, log_failure] >> join

    # ── Branch: stop pipeline if staging failed ────────────────────────────────

    @task.branch(task_id='branch_after_staging', trigger_rule=TriggerRule.ALL_DONE)
    def branch_after_staging(**context):
        """
        Checks the exit state of dbt-staging-job.
        Routes to transform if successful, failure email if not.
        """
        from airflow.utils.state import State

        task_instance = context['dag_run'].get_task_instance(
            'test_raw_data_group.execute_staging_job'
        )
        if task_instance and task_instance.state == State.SUCCESS:
            logger.info("Staging job passed — proceeding to transform.")
            return 'transform_data_group.execute_transform_job'

        logger.error("Staging job failed — routing to failure email.")
        return 'send_failure_email'

    # ── Task Group 2: Transform ────────────────────────────────────────────────
    # dbt-transform-job runs: dbt run --select marts

    @task_group(group_id='transform_data_group')
    def transform_data_group():

        execute_transform_job = CloudRunExecuteJobOperator(
            task_id='execute_transform_job',
            project_id=PROJECT_ID,
            region=REGION,
            job_name='dbt-transform-job',
            deferrable=True,
        )

        log_success = _make_log_task(
            task_id='log_transform_success',
            job_name='dbt-transform-job',
            status='SUCCESS ✅',
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )()

        log_failure = _make_log_task(
            task_id='log_transform_failure',
            job_name='dbt-transform-job',
            status='FAILED ❌',
            trigger_rule=TriggerRule.ONE_FAILED,
        )()

        join = EmptyOperator(
            task_id='join_transform',
            trigger_rule=TriggerRule.ALL_DONE,
        )

        execute_transform_job >> [log_success, log_failure] >> join

    # ── Task Group 3: Marts Test ───────────────────────────────────────────────
    # dbt-marts-test-job runs: dbt test --select marts

    @task_group(group_id='test_marts_group')
    def test_marts_group():

        execute_marts_test_job = CloudRunExecuteJobOperator(
            task_id='execute_marts_test_job',
            project_id=PROJECT_ID,
            region=REGION,
            job_name='dbt-marts-test-job',
            deferrable=True,
        )

        log_success = _make_log_task(
            task_id='log_marts_test_success',
            job_name='dbt-marts-test-job',
            status='ALL TESTS PASSED ✅',
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )()

        log_failure = _make_log_task(
            task_id='log_marts_test_failure',
            job_name='dbt-marts-test-job',
            status='TESTS FAILED ❌',
            trigger_rule=TriggerRule.ONE_FAILED,
        )()

        join = EmptyOperator(
            task_id='join_marts_test',
            trigger_rule=TriggerRule.ALL_DONE,
        )

        execute_marts_test_job >> [log_success, log_failure] >> join

    # ── Email operators ────────────────────────────────────────────────────────
    # EmailOperator is a traditional operator — stays as-is in TaskFlow DAGs.
    # @task cannot wrap operators, only Python callables.

    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to=ALERT_EMAIL,
        subject='[FAILED] ELT Pipeline — {{ dag.dag_id }} — {{ ds }}',
        html_content="""
        <h2 style="color:red;">❌ Stratum ELT Pipeline Failed</h2>
        <table>
            <tr><td><b>DAG</b></td><td>{{ dag.dag_id }}</td></tr>
            <tr><td><b>Execution Date</b></td><td>{{ ds }}</td></tr>
            <tr><td><b>Run ID</b></td><td>{{ run_id }}</td></tr>
        </table>
        <p>Check the Airflow logs in Cloud Composer for details.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    send_success_email = EmailOperator(
        task_id='send_success_email',
        to=ALERT_EMAIL,
        subject='[SUCCESS] ELT Pipeline — {{ dag.dag_id }} — {{ ds }}',
        html_content="""
        <h2 style="color:green;">✅ Stratum ELT Pipeline Completed Successfully</h2>
        <table>
            <tr><td><b>DAG</b></td><td>{{ dag.dag_id }}</td></tr>
            <tr><td><b>Execution Date</b></td><td>{{ ds }}</td></tr>
            <tr><td><b>Run ID</b></td><td>{{ run_id }}</td></tr>
        </table>
        <ul>
            <li>✅ Raw data freshness checked</li>
            <li>✅ Source tests passed</li>
            <li>✅ Bad rows quarantined</li>
            <li>✅ Staging materialised</li>
            <li>✅ Mart transformations complete</li>
            <li>✅ All mart tests passed</li>
        </ul>
        <p>Data is ready in BigQuery.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Trigger Dataplex DAG ───────────────────────────────────────────────────

    # trigger_dataplex_dag = TriggerDagRunOperator(
    #     task_id='trigger_dataplex_quality_dag',
    #     trigger_dag_id='stratum_dataplex_quality',
    #     wait_for_completion=True,
    #     deferrable=True,
    #     failed_states=['failed'],
    #     trigger_rule=TriggerRule.ALL_SUCCESS,
    # )

    # ── Dependencies ───────────────────────────────────────────────────────────

    staging_group   = test_raw_data_group()
    branch          = branch_after_staging()
    transform_group = transform_data_group()
    marts_group     = test_marts_group()

    # main flow
    start >> staging_group >> branch

    # happy path
    branch >> transform_group >> marts_group
    marts_group >> send_success_email >> end

    # failure paths
    branch >> send_failure_email
    marts_group >> send_failure_email
    send_failure_email >> end


# instantiate the DAG
transform()