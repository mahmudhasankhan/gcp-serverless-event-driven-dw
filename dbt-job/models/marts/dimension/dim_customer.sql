{# 
{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        on_schema_change='fail'
    )
}} #}


WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

customer_deduped as (
    SELECT distinct 
        customer_id,
        customer_name,
    FROM sales
),

final AS (
    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,

        -- Descriptive Values
        customer_id,
        customer_name,
    FROM customer_deduped 
)

SELECT * FROM final