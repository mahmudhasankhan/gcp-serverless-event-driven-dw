-- Test: Revenue should be approximately equal to unit_price * quantity
-- Allows 1% tolerance for rounding differences
-- Any rows returned = test FAILS

SELECT
    sale_key,
    order_id,
    unit_price,
    quantity,
    revenue,
    ROUND(unit_price * quantity, 2)  AS expected_revenue,
    ABS(revenue - (unit_price * quantity)) AS discrepancy
FROM {{ ref('fct_grocery_sales') }}
WHERE ABS(revenue - (unit_price * quantity)) > (unit_price * quantity * 0.01)
