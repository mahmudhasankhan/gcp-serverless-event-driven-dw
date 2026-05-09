-- Test: Monthly revenue model must have exactly one row per year + month
-- Duplicates mean the GROUP BY in the model is broken
-- Any rows returned = test FAILS

SELECT
    year,
    month,
    COUNT(*) AS row_count,
    'duplicate year+month in monthly_revenue_growth' AS issue
FROM {{ ref('monthly_revenue_growth') }}
GROUP BY year, month
HAVING COUNT(*) > 1
