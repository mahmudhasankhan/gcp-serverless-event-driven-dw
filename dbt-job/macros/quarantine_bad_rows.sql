{% macro quarantine_bad_rows() %}

/*
    quarantine_bad_rows()
    ─────────────────────
    Standalone operation — called via:
        dbt run-operation quarantine_bad_rows

    Reads raw_data.sales, identifies rows that fail quality rules,
    and writes them to quarantine.stg_sales_rejected_YYYYMMDD in BigQuery.

    Runs BEFORE dbt run --select staging so bad rows are captured
    before the model filters them out silently.
*/

{%- set date_suffix = run_started_at.strftime('%Y%m%d') -%}
{%- set destination = target.project ~ '.quarantine.stg_sales_rejected_' ~ date_suffix -%}

{%- set count_sql -%}
    SELECT COUNT(*) AS cnt
    FROM `{{ target.project }}.raw_data.sales`
    WHERE order_id IS NULL
       OR customer_id IS NULL
       OR unit_price IS NULL OR CAST(unit_price AS NUMERIC)  <= 0
       OR quantity IS NULL OR CAST(quantity   AS INTEGER)  <= 0
       OR revenue IS NULL OR CAST(revenue    AS NUMERIC)  < 0
       OR CAST(shipped_date AS DATE) < CAST(order_date AS DATE)
{%- endset -%}

{% set count_result = run_query(count_sql) %}
{% set bad_row_count = count_result.columns[0].values()[0] %}

{% if bad_row_count > 0 %}

    {% do log(bad_row_count ~ " bad rows found — writing to " ~ destination, info=true) %}

    {%- set quarantine_sql -%}
        CREATE OR REPLACE TABLE `{{ destination }}` AS
        SELECT
            *,
            CURRENT_TIMESTAMP() AS quarantined_at,
            CASE
                WHEN order_id IS NULL THEN 'null order_id'
                WHEN customer_id IS NULL THEN 'null customer_id'
                WHEN unit_price IS NULL THEN 'null unit_price'
                WHEN CAST(unit_price AS NUMERIC) <= 0 THEN 'unit_price <= 0'
                WHEN quantity IS NULL THEN 'null quantity'
                WHEN CAST(quantity AS INTEGER) <= 0 THEN 'quantity <= 0'
                WHEN revenue IS NULL THEN 'null revenue'
                WHEN CAST(revenue AS NUMERIC) < 0 THEN 'negative revenue'
                WHEN CAST(shipped_date AS DATE) < CAST(order_date AS DATE) THEN 'shipped before ordered'
                ELSE 'unknown'
            END AS failed_condition
        FROM `{{ target.project }}.raw_data.sales`
        WHERE order_id IS NULL
           OR customer_id IS NULL
           OR unit_price IS NULL OR CAST(unit_price AS NUMERIC)  <= 0
           OR quantity IS NULL OR CAST(quantity   AS INTEGER)  <= 0
           OR revenue IS NULL OR CAST(revenue    AS NUMERIC)  < 0
           OR CAST(shipped_date AS DATE) < CAST(order_date AS DATE)
    {%- endset -%}

    {% do run_query(quarantine_sql) %}
    {% do log("Quarantine table written: " ~ destination, info=true) %}

{% else %}
    {% do log("No bad rows found — quarantine skipped.", info=true) %}
{% endif %}

{% endmacro %}