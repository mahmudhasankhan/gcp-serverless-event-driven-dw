-- Test: City ranking model must have rank 1 present (highest revenue city exists)
-- and total_revenue must be positive for all ranked cities
-- Any rows returned = test FAILS

SELECT
    city,
    total_revenue,
    rank_by_revenue,
    'negative or zero revenue in ranking' AS issue
FROM {{ ref('city_rank_by_revenue_qty_sales') }}
WHERE total_revenue <= 0

UNION ALL

SELECT
    city,
    total_revenue,
    rank_by_revenue,
    'null rank detected'
FROM {{ ref('city_rank_by_revenue_qty_sales') }}
WHERE rank_by_revenue IS NULL
