-- Test: YTD revenue must always be greater than or equal to daily revenue
-- If ytd_revenue < daily_revenue, the cumulative window function is broken
-- Any rows returned = test FAILS

SELECT
    date,
    year,
    daily_revenue,
    ytd_revenue,
    'ytd_revenue less than daily_revenue — window function error' AS issue
FROM {{ ref('ytd_revenue_qty_shipment_growth') }}
WHERE ytd_revenue < daily_revenue
