{# {{
    config(
        materialized='incremental',
        unique_key='sales_person_key',
        on_schema_change='fail'
    )
}} #}


WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),
salesperson_deduped AS (
    SELECT DISTINCT 
    sales_person 
    FROM sales 
),

final AS (

    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['sales_person']) }} AS sales_person_key,

        -- Descriptive Values
        sales_person 
    FROM salesperson_deduped 
)

SELECT * FROM final