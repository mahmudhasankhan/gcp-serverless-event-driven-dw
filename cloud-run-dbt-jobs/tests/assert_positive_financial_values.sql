-- Test: Financial columns should have positive values
-- Negative values would indicate data quality issues

SELECT
    order_id,
    product_name,
    unit_price,
    quantity,
    revenue,
    shipping_fee
FROM {{ ref('stg_sales') }} 
WHERE (unit_price IS NOT NULL AND unit_price < 0)
   OR (quantity IS NOT NULL AND quantity <= 0)
   OR (revenue IS NOT NULL AND revenue < 0)
   OR (shipping_fee IS NOT NULL AND shipping_fee < 0)
