{# 
{{
    config(
        materialized='incremental',
        unique_key='product_key',
        on_schema_change='fail'
    )
}} #}


WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

product_deduped AS (
    SELECT DISTINCT 
    product_name,
    category
    FROM sales 
),

final AS (

    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['product_name']) }} AS product_key,

        -- Descriptive Values
        product_name,
        category
    FROM product_deduped
)

SELECT * FROM final