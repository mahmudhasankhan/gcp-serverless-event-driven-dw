

WITH sales AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

{# dates AS (
    SELECT * FROM {{ ref('dim_date') }}
), #}

shipper AS (
    SELECT * FROM {{ ref('dim_shipper') }}
),


final AS (
    SELECT
        s.shipper_name,
        SUM(f.shipping_fee) AS total_shipping_fees,
        SUM(f.sale_count) AS total_shipments,
        ROUND(AVG(f.shipping_fee), 2) AS avg_shipping_fee_per_shipment,
        SUM(f.revenue) AS total_revenue,
        ROUND(SUM(f.shipping_fee) / NULLIF(SUM(f.revenue), 0) * 100, 2) AS shipping_cost_percent_of_revenue
    FROM sales f
    JOIN shipper s ON f.shipper_key = s.shipper_key
    GROUP BY s.shipper_name
)

SELECT * FROM final ORDER BY total_shipping_fees DESC
