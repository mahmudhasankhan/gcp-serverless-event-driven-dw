-- Test: Shipped date should always be on or after the order date
-- This test will FAIL if any records violate this logic

SELECT
    order_date,
    shipped_date
FROM {{ ref('stg_sales') }}
WHERE shipped_date < order_date