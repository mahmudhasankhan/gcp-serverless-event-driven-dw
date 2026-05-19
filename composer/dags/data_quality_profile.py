"""

Triggered by transformation_pipeline DAG after all three dbt Cloud Run
Jobs complete successfully.

Scans run in parallel:
    fct_quality_scan_group      → structural integrity on fct_grocery_sales
    revenue_marts_scan_group    → monthly + quarterly + yearly revenue models
    growth_models_scan_group    → ytd + mtd + qtd growth models
    rankings_scan_group         → all 5 ranking models
    shipment_scan_group         → shipment expenditure comparison model
    profile_scan_group          → column statistics on fct_grocery_sales

All scan results exported to warehouse.dq_results in BigQuery.
A single consolidated summary email is sent after all scans complete.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task_group, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateOrUpdateDataQualityScanOperator,
    DataplexCreateOrUpdateDataProfileScanOperator,
    DataplexRunDataQualityScanOperator,
    DataplexRunDataProfileScanOperator,
    DataplexGetDataQualityScanResultOperator,
    DataplexGetDataProfileScanResultOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID  = 'sales-datawarehouse'
REGION      = os.getenv('REGION')
ALERT_EMAIL = os.getenv('ALERT_EMAIL')
DATASET     = 'warehouse'

logger = logging.getLogger(__name__)


def _bq_resource(table: str) -> str:
    """Build the BigQuery resource path for a Dataplex scan."""
    return (
        f'//bigquery.googleapis.com/projects/{PROJECT_ID}'
        f'/datasets/{DATASET}/tables/{table}'
    )


def _results_table() -> str:
    """BigQuery table where all scan results are exported."""
    return (
        f'//bigquery.googleapis.com/projects/{PROJECT_ID}'
        f'/datasets/{DATASET}/tables/dq_results'
    )


def _base_scan_body(table: str, rules: list) -> dict:
    """
    Base quality scan body shared across all quality scans.
    Exports results to warehouse.dq_results for dashboarding.
    """
    return {
        'data': {'resource': _bq_resource(table)},
        'data_quality_spec': {
            'rules': rules,
            'post_scan_actions': {
                'bigquery_export': {
                    'results_table': _results_table()
                }
            }
        },
        'execution_spec': {'trigger': {'on_demand': {}}}
    }


default_args = {
    'owner':            'data-team',
    'depends_on_past':  False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry':   False,
}


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id='automated_data_quality_check_and_profile_scan_pipeline',
    default_args=default_args,
    description='Knowledge catalog quality and profile scans',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=6,
    tags=['dataplex', 'data-quality', 'knowledge-catalog'],
)
def dataplex_quality():

    start = EmptyOperator(task_id='start')

    # ── Scan Group 1: fct_grocery_sales quality ───────────────────────────────
    # Structural integrity — PK uniqueness, FK completeness, financial validity
    # Most critical scan — every downstream mart reads from this table

    @task_group(group_id='fct_quality_scan_group')
    def fct_quality_scan_group():

        create_or_update = DataplexCreateOrUpdateDataQualityScanOperator(
            task_id='create_or_update_fct_quality_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='fct-quality-scan',
            body=_base_scan_body('fct_grocery_sales', rules=[

                # ── Primary key ──────────────────────────────────────────────
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-sale-key-not-null',
                    'description': 'sale_key must not be null — surrogate PK',
                    'column':      'sale_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'UNIQUENESS',
                    'name':        'fct-sale-key-unique',
                    'description': 'sale_key must be unique — no duplicate rows',
                    'column':      'sale_key',
                    'threshold':   1.0,
                    'uniqueness_expectation': {}
                },

                # ── Natural key ───────────────────────────────────────────────
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-order-id-not-null',
                    'description': 'order_id must not be null',
                    'column':      'order_id',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },

                # ── Foreign keys ──────────────────────────────────────────────
                # FK relationship integrity is handled by dbt tests
                # Dataplex checks these columns are always populated (not null)
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-customer-key-not-null',
                    'description': 'customer_key FK must not be null',
                    'column':      'customer_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-product-key-not-null',
                    'description': 'product_key FK must not be null',
                    'column':      'product_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-salesperson-key-not-null',
                    'description': 'salesperson_key FK must not be null',
                    'column':      'salesperson_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-shipper-key-not-null',
                    'description': 'shipper_key FK must not be null',
                    'column':      'shipper_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-region-key-not-null',
                    'description': 'region_key FK must not be null',
                    'column':      'region_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-payment-type-key-not-null',
                    'description': 'payment_type_key FK must not be null',
                    'column':      'payment_type_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-order-date-key-not-null',
                    'description': 'order_date_key FK to dim_date must not be null',
                    'column':      'order_date_key',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },

                # ── Financial measures ────────────────────────────────────────
                {
                    'dimension':   'VALIDITY',
                    'name':        'fct-revenue-positive',
                    'description': 'revenue must be greater than 0',
                    'column':      'revenue',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value':          '0',
                        'strict_min_enabled': True,
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'fct-unit-price-positive',
                    'description': 'unit_price must be greater than 0',
                    'column':      'unit_price',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value':          '0',
                        'strict_min_enabled': True,
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'fct-quantity-positive',
                    'description': 'quantity must be greater than 0',
                    'column':      'quantity',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value':          '0',
                        'strict_min_enabled': True,
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'fct-shipping-fee-non-negative',
                    'description': 'shipping_fee must be >= 0 (free shipping is valid)',
                    'column':      'shipping_fee',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0',
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'fct-sale-count-always-one',
                    'description': 'sale_count must always equal 1 — deviation means model bug',
                    'column':      'sale_count',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '1',
                        'max_value': '1',
                    }
                },

                # ── Audit columns ─────────────────────────────────────────────
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-batch-id-not-null',
                    'description': 'batch_id must not be null — monthly audit trail',
                    'column':      'batch_id',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'fct-loaded-at-not-null',
                    'description': 'loaded_at timestamp must not be null',
                    'column':      'loaded_at',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
            ])
        )

        run = DataplexRunDataQualityScanOperator(
            task_id='run_fct_quality_scan',
            project_id=PROJECT_ID,
            region=REGION,
            deferrable=True,
            data_scan_id='fct-quality-scan',
        )

        get_results = DataplexGetDataQualityScanResultOperator(
            task_id='get_fct_quality_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='fct-quality-scan',
            job_id="{{ task_instance.xcom_pull(task_ids='fct_quality_scan_group.run_fct_quality_scan').split('/')[-1] }}",
        )

        create_or_update >> run >> get_results

    # ── Scan Group 2: Revenue mart models ─────────────────────────────────────
    # monthly_revenue_growth, quarterly_revenue_growth, yearly_revenue_growth
    # Run in parallel within the group

    @task_group(group_id='revenue_marts_scan_group')
    def revenue_marts_scan_group():

        # Monthly
        create_monthly = DataplexCreateOrUpdateDataQualityScanOperator(
            task_id='create_or_update_monthly_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='monthly-revenue-scan',
            body=_base_scan_body('monthly_revenue_growth', rules=[
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'monthly-year-not-null',
                    'column':      'year',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'monthly-month-not-null',
                    'column':      'month',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'monthly-month-name-not-null',
                    'column':      'month_name',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'monthly-revenue-positive',
                    'description': 'total_revenue must be greater than 0',
                    'column':      'total_revenue',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'monthly-growth-rate-in-range',
                    'description': 'mom_growth_rate outside -100 to 500 means LAG() broke',
                    'column':      'mom_growth_rate',
                    'threshold':   0.95,   # first row has null prev_month — expected
                    'range_expectation': {
                        'min_value': '-100',
                        'max_value': '500',
                    }
                },
            ])
        )
        run_monthly = DataplexRunDataQualityScanOperator(
            task_id='run_monthly_scan',
            project_id=PROJECT_ID,
            region=REGION,
            deferrable=True,
            data_scan_id='monthly-revenue-scan',
        )
        get_monthly = DataplexGetDataQualityScanResultOperator(
            task_id='get_monthly_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='monthly-revenue-scan',
            job_id="{{ task_instance.xcom_pull(task_ids='revenue_marts_scan_group.run_monthly_scan').split('/')[-1] }}",
        )

        # Quarterly
        create_quarterly = DataplexCreateOrUpdateDataQualityScanOperator(
            task_id='create_or_update_quarterly_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='quarterly-revenue-scan',
            body=_base_scan_body('quarterly_revenue_growth', rules=[
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'quarterly-year-not-null',
                    'column':      'year',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'quarterly-quarter-not-null',
                    'column':      'quarter',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'quarterly-quarter-valid-range',
                    'description': 'quarter must be between 1 and 4',
                    'column':      'quarter',
                    'threshold':   1.0,
                    'range_expectation': {'min_value': '1', 'max_value': '4'}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'quarterly-revenue-positive',
                    'column':      'total_revenue',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'quarterly-growth-rate-in-range',
                    'description': 'qoq_growth_rate outside -100 to 500 means LAG() broke',
                    'column':      'qoq_growth_rate',
                    'threshold':   0.95,
                    'range_expectation': {'min_value': '-100', 'max_value': '500'}
                },
            ])
        )
        run_quarterly = DataplexRunDataQualityScanOperator(
            task_id='run_quarterly_scan',
            project_id=PROJECT_ID,
            region=REGION,
            deferrable=True,
            data_scan_id='quarterly-revenue-scan',
        )
        get_quarterly = DataplexGetDataQualityScanResultOperator(
            task_id='get_quarterly_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='quarterly-revenue-scan',
            job_id="{{ task_instance.xcom_pull(task_ids='revenue_marts_scan_group.run_quarterly_scan').split('/')[-1] }}",
        )

        # Yearly
        create_yearly = DataplexCreateOrUpdateDataQualityScanOperator(
            task_id='create_or_update_yearly_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='yearly-revenue-scan',
            body=_base_scan_body('yearly_revenue_growth', rules=[
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'yearly-year-not-null',
                    'column':      'year',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'UNIQUENESS',
                    'name':        'yearly-year-unique',
                    'description': 'year must be unique — one row per year',
                    'column':      'year',
                    'threshold':   1.0,
                    'uniqueness_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'yearly-revenue-positive',
                    'column':      'total_revenue',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'yearly-growth-rate-in-range',
                    'description': 'yoy_growth_rate outside -100 to 500 means LAG() broke',
                    'column':      'yoy_growth_rate',
                    'threshold':   0.95,
                    'range_expectation': {'min_value': '-100', 'max_value': '500'}
                },
            ])
        )
        run_yearly = DataplexRunDataQualityScanOperator(
            task_id='run_yearly_scan',
            project_id=PROJECT_ID,
            region=REGION,
            deferrable=True,
            data_scan_id='yearly-revenue-scan',
        )
        get_yearly = DataplexGetDataQualityScanResultOperator(
            task_id='get_yearly_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='yearly-revenue-scan',
            job_id="{{ task_instance.xcom_pull(task_ids='revenue_marts_scan_group.run_yearly_scan').split('/')[-1] }}",
        )

        # Step 1: Force creation tasks into a strict, quota-safe line
        create_monthly >> create_quarterly >> create_yearly

        # All three run in parallel within this group
        create_monthly   >> run_monthly   >> get_monthly
        create_quarterly >> run_quarterly >> get_quarterly
        create_yearly    >> run_yearly    >> get_yearly

    # ── Scan Group 3: Growth models (YTD / MTD / QTD) ────────────────────────
    # Same rule pattern across all three — reused via factory function

    @task_group(group_id='growth_models_scan_group')
    def growth_models_scan_group():

        def _growth_rules(prefix: str, cumulative_col: str) -> list:
            return [
                {
                    'dimension':   'COMPLETENESS',
                    'name':        f'{prefix}-date-not-null',
                    'description': 'date must not be null',
                    'column':      'date',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'UNIQUENESS',
                    'name':        f'{prefix}-date-unique',
                    'description': 'one row per date — duplicates mean GROUP BY broke',
                    'column':      'date',
                    'threshold':   1.0,
                    'uniqueness_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        f'{prefix}-daily-revenue-positive',
                    'column':      'daily_revenue',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        f'{prefix}-cumulative-revenue-not-null',
                    'description': f'{cumulative_col} null means window function failed',
                    'column':      cumulative_col,
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        f'{prefix}-cumulative-revenue-positive',
                    'column':      cumulative_col,
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
            ]

        for prefix, table, cum_col in [
            ('ytd', 'ytd_revenue_qty_shipment_growth', 'ytd_revenue'),
            ('mtd', 'mtd_revenue_qty_shipment_growth', 'mtd_revenue'),
            ('qtd', 'qtd_revenue_qty_shipment_growth', 'qtd_revenue'),
        ]:
            create = DataplexCreateOrUpdateDataQualityScanOperator(
                task_id=f'create_or_update_{prefix}_scan',
                project_id=PROJECT_ID,
                region=REGION,
                data_scan_id=f'{prefix}-scan',
                body=_base_scan_body(table, rules=_growth_rules(prefix, cum_col))
            )
            run = DataplexRunDataQualityScanOperator(
                task_id=f'run_{prefix}_scan',
                project_id=PROJECT_ID,
                region=REGION,
                deferrable=True,
                data_scan_id=f'{prefix}-scan',
            )
            get = DataplexGetDataQualityScanResultOperator(
                task_id=f'get_{prefix}_results',
                project_id=PROJECT_ID,
                region=REGION,
                data_scan_id=f'{prefix}-scan',
                job_id=f"{{{{ task_instance.xcom_pull(task_ids='growth_models_scan_group.run_{prefix}_scan').split('/')[-1] }}}}",
            )
            create >> run >> get

    # ── Scan Group 4: Rankings ────────────────────────────────────────────────
    # All 5 ranking models — city, customer, product, region, salesperson
    # Same rule pattern reused via factory function

    @task_group(group_id='rankings_scan_group')
    def rankings_scan_group():

        def _ranking_rules(prefix: str, dim_col: str) -> list:
            return [
                {
                    'dimension':   'COMPLETENESS',
                    'name':        f'{prefix}-dim-not-null',
                    'column':      dim_col,
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'UNIQUENESS',
                    'name':        f'{prefix}-dim-unique',
                    'description': f'one row per {dim_col}',
                    'column':      dim_col,
                    'threshold':   1.0,
                    'uniqueness_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        f'{prefix}-revenue-positive',
                    'column':      'total_revenue',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
                {
                    'dimension':   'COMPLETENESS',
                    'name':        f'{prefix}-rank-not-null',
                    'description': 'null rank means RANK() OVER broke',
                    'column':      'rank_by_revenue',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        f'{prefix}-rank-positive',
                    'description': 'rank_by_revenue must be >= 1',
                    'column':      'rank_by_revenue',
                    'threshold':   1.0,
                    'range_expectation': {'min_value': '1'}
                },
            ]

        for prefix, table, dim_col in [
            ('city',        'city_rank_by_revenue_qty_sales',       'city'),
            ('customer',    'customer_rank_by_revenue_qty_sales',    'customer_name'),
            ('product',     'product_rank_by_revenue_qty_sales',     'product_name'),
            ('region',      'region_rank_by_revenue_qty_sales',      'region'),
            ('salesperson', 'salesperson_rank_by_revenue_qty_sales', 'sales_person'),
        ]:
            create = DataplexCreateOrUpdateDataQualityScanOperator(
                task_id=f'create_or_update_{prefix}_ranking_scan',
                project_id=PROJECT_ID,
                region=REGION,
                data_scan_id=f'{prefix}-ranking-scan',
                body=_base_scan_body(table, rules=_ranking_rules(prefix, dim_col))
            )
            run = DataplexRunDataQualityScanOperator(
                task_id=f'run_{prefix}_ranking_scan',
                project_id=PROJECT_ID,
                region=REGION,
                deferrable=True,
                data_scan_id=f'{prefix}-ranking-scan',
            )
            get = DataplexGetDataQualityScanResultOperator(
                task_id=f'get_{prefix}_ranking_results',
                project_id=PROJECT_ID,
                region=REGION,
                data_scan_id=f'{prefix}-ranking-scan',
                job_id=f"{{{{ task_instance.xcom_pull(task_ids='rankings_scan_group.run_{prefix}_ranking_scan').split('/')[-1] }}}}",
            )
            create >> run >> get

    # ── Scan Group 5: Shipment expenditure ───────────────────────────────────
    # shipment_exp_comparison_by_ship_company
    # shipping_cost_percent_of_revenue uses NULLIF — threshold 0.95 to tolerate
    # legitimate nulls when revenue=0 for a shipper

    @task_group(group_id='shipment_scan_group')
    def shipment_scan_group():

        create = DataplexCreateOrUpdateDataQualityScanOperator(
            task_id='create_or_update_shipment_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='shipment-scan',
            body=_base_scan_body('shipment_exp_comparison_by_ship_company', rules=[
                {
                    'dimension':   'COMPLETENESS',
                    'name':        'shipment-name-not-null',
                    'column':      'shipper_name',
                    'threshold':   1.0,
                    'non_null_expectation': {}
                },
                {
                    'dimension':   'UNIQUENESS',
                    'name':        'shipment-name-unique',
                    'description': 'one row per shipper',
                    'column':      'shipper_name',
                    'threshold':   1.0,
                    'uniqueness_expectation': {}
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'shipment-total-fees-positive',
                    'column':      'total_shipping_fees',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'shipment-total-shipments-positive',
                    'column':      'total_shipments',
                    'threshold':   1.0,
                    'range_expectation': {
                        'min_value': '0', 'strict_min_enabled': True
                    }
                },
                {
                    'dimension':   'VALIDITY',
                    'name':        'shipment-cost-percent-in-range',
                    'description': 'shipping_cost_percent_of_revenue should be 0-100',
                    'column':      'shipping_cost_percent_of_revenue',
                    'threshold':   0.95,   # NULLIF produces nulls when revenue=0
                    'range_expectation': {'min_value': '0', 'max_value': '100'}
                },
            ])
        )

        run = DataplexRunDataQualityScanOperator(
            task_id='run_shipment_scan',
            project_id=PROJECT_ID,
            region=REGION,
            deferrable=True,
            data_scan_id='shipment-scan',
        )

        get = DataplexGetDataQualityScanResultOperator(
            task_id='get_shipment_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='shipment-scan',
            job_id="{{ task_instance.xcom_pull(task_ids='shipment_scan_group.run_shipment_scan').split('/')[-1] }}",
        )

        create >> run >> get

    # ── Scan Group 6: Profile scan ────────────────────────────────────────────
    # Column statistics on fct_grocery_sales
    # Min, max, mean, null counts, distinct counts, top values
    # Most useful for detecting data drift across monthly loads

    @task_group(group_id='profile_scan_group')
    def profile_scan_group():

        create = DataplexCreateOrUpdateDataProfileScanOperator(
            task_id='create_or_update_profile_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='fct-profile-scan',
            body={
                'data': {'resource': _bq_resource('fct_grocery_sales')},
                'data_profile_spec': {},   # empty = full default profile on all columns
                'execution_spec': {'trigger': {'on_demand': {}}}
            }
        )

        run = DataplexRunDataProfileScanOperator(
            task_id='run_profile_scan',
            project_id=PROJECT_ID,
            region=REGION,
            deferrable=True,
            data_scan_id='fct-profile-scan',
        )

        get = DataplexGetDataProfileScanResultOperator(
            task_id='get_profile_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id='fct-profile-scan',
        )

        create >> run >> get

    # ── Summary email ─────────────────────────────────────────────────────────
    # Single consolidated email after all six scan groups complete

    send_summary_email = EmailOperator(
        task_id='send_summary_email',
        to=ALERT_EMAIL,
        subject='Dataplex Quality Scan Results — {{ ds }}',
        html_content="""
        <h2>Knowledge Catalog Scan Results</h2>
        <table border="1" cellpadding="6" cellspacing="0">
            <tr><td><b>DAG</b></td><td>{{ dag.dag_id }}</td></tr>
            <tr><td><b>Execution Date</b></td><td>{{ ds }}</td></tr>
            <tr><td><b>Run ID</b></td><td>{{ run_id }}</td></tr>
        </table>

        <h3>Tables scanned</h3>
        <ul>
            <li>fct_grocery_sales — structural integrity (PK, FKs, financial measures)</li>
            <li>monthly_revenue_growth / quarterly_revenue_growth / yearly_revenue_growth</li>
            <li>ytd / mtd / qtd revenue growth models</li>
            <li>city / customer / product / region / salesperson rankings</li>
            <li>shipment_exp_comparison_by_ship_company</li>
            <li>fct_grocery_sales — column profile (data drift detection)</li>
        </ul>

        <h3>fct_grocery_sales quality results</h3>
        <pre>{{ task_instance.xcom_pull(
            task_ids='fct_quality_scan_group.get_fct_quality_results'
        ) | tojson(indent=4) }}</pre>

        <p>
            Full results for all scans exported to
            <b>warehouse.dq_results</b> in BigQuery.
        </p>
        """,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    @task(task_id='log_pipeline_success', trigger_rule=TriggerRule.ALL_SUCCESS)
    def log_pipeline_success(**context):
        logger.info(
            f"\n{'='*42}\n"
            f"🎉 DATA QUALITY PIPELINE COMPLETED SUCCESSFULLY 🎉\n"
            f"{'='*42}\n"
            f"DAG            : {context['dag'].dag_id}\n"
            f"Execution Date : {context['ds']}\n"
            f"Run ID         : {context['run_id']}\n"
            f"{'='*42}"
        )
    
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────

    fct_group      = fct_quality_scan_group()
    revenue_group  = revenue_marts_scan_group()
    # growth_group   = growth_models_scan_group()
    # rankings_group = rankings_scan_group()
    # shipment_group = shipment_scan_group()
    profile_group  = profile_scan_group()
    log_pipeline = log_pipeline_success()

    # start >> fct_group >> revenue_group >> growth_group >> rankings_group >> shipment_group >> log_pipeline
    start >> fct_group >> revenue_group >> log_pipeline
    start >> profile_group >> log_pipeline 
    log_pipeline >> send_summary_email >> end


dataplex_quality()