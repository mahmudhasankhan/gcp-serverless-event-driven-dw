{# {{
    config(
        materialized='incremental',
        unique_key='sale_key',
        on_schema_change='fail'
    )
}} #}


WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    {# {% if is_incremental() %} #}
    -- Only process new batches that don't exist in the fact table yet
    {# WHERE batch_id NOT IN (SELECT DISTINCT batch_id FROM {{ this }}) #}
    {# {% endif %} #}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (

   SELECT 
        -- Surrogate Key 
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_name']) }} AS sale_key,

        -- Natural Key
        s.order_id,
        -- Foreign keys to dimensions
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['product_name']) }} AS product_key,
        {{ dbt_utils.generate_surrogate_key(['sales_person']) }} AS salesperson_key,
        {{ dbt_utils.generate_surrogate_key(['shipper_name']) }} AS shipper_key,
        {{ dbt_utils.generate_surrogate_key(['city', 'state', 'country', 'region']) }} AS region_key,
        {{ dbt_utils.generate_surrogate_key(['payment_type']) }} AS payment_type_key,

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