{{
    config(
        materialized='incremental',
        unique_key='sale_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    {% if is_incremental() %}
    -- Only scan data that was loaded into staging AFTER the last time this fact table updated
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (

   SELECT 
        -- Surrogate Key 
        {{ dbt_utils.generate_surrogate_key(['s.order_id', 's.product_name']) }} AS sale_key,

        -- Natural Key
        s.order_id,
        
        -- Foreign keys to dimensions (Added 's.' aliases here)
        {{ dbt_utils.generate_surrogate_key(['s.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['s.product_name']) }} AS product_key,
        {{ dbt_utils.generate_surrogate_key(['s.sales_person']) }} AS salesperson_key,
        {{ dbt_utils.generate_surrogate_key(['s.shipper_name']) }} AS shipper_key,
        {{ dbt_utils.generate_surrogate_key(['s.city', 's.state', 's.country', 's.region']) }} AS region_key,
        {{ dbt_utils.generate_surrogate_key(['s.payment_type']) }} AS payment_type_key,

        -- Date foreign keys
        d.date_key AS order_date_key,
        dd.date_key AS shipped_date_key,

        -- Measures
        s.unit_price,
        s.quantity,
        s.revenue,
        s.shipping_fee,

        -- derived fact
        1 AS sale_count,

        -- audit columns
        s.batch_id,
        s.loaded_at

    FROM sales s
    LEFT JOIN dates d ON s.order_date = d.date
    LEFT JOIN dates dd ON s.shipped_date = dd.date
)

SELECT * FROM final