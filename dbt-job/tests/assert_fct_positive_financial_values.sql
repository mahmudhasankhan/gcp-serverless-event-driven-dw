-- Test: All financial measures in the fact table must be positive
-- Negative revenue, price or quantity indicates a transformation error
-- Any rows returned = test FAILS

SELECT
    sale_key,
    order_id,
    unit_price,
    quantity,
    revenue,
    shipping_fee
FROM {{ ref('fct_grocery_sales') }}
WHERE unit_price    <= 0
   OR quantity      <= 0
   OR revenue       <= 0
   OR shipping_fee  <  0
