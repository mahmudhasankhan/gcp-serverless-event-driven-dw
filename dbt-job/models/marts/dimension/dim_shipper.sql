{# {{
    config(
        materialized='incremental',
        unique_key='shipper_key',
        on_schema_change='fail'
    )
}} #}


WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

shipper_deduped AS (
    SELECT DISTINCT 
        shipper_name 
    FROM sales 
),

final AS (

    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['shipper_name']) }} AS shipper_key,

        -- Descriptive Values
        shipper_name 
    FROM shipper_deduped 
)

SELECT * FROM final