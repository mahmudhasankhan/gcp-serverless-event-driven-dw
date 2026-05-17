-- Test: Each order_id should map to exactly one customer_id
-- Orders can have multiple line items, but all should belong to same customer

SELECT
order_id,
COUNT(DISTINCT customer_id) AS customer_count,
FROM {{ ref('stg_sales') }}
GROUP BY order_id
HAVING customer_count > 1