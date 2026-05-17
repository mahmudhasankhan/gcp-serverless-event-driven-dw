
WITH sales AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

quarterly_revenue AS (
    SELECT
        d.year,
        d.quarter,
        d.quarter_name,
        SUM(s.revenue) AS total_revenue
    FROM sales s
    LEFT JOIN dates d ON s.order_date_key = d.date_key
    GROUP BY d.year, d.quarter, d.quarter_name
),

comparison AS (
    SELECT
        *,
        LAG(total_revenue) OVER (ORDER BY year, quarter) AS prev_quarter_revenue,
        ROUND(SAFE_DIVIDE(
            total_revenue - LAG(total_revenue) OVER (ORDER BY year, quarter),
            LAG(total_revenue) OVER (ORDER BY year, quarter)
        ) * 100, 2) AS qoq_growth_rate
    FROM quarterly_revenue
)

SELECT * FROM comparison
ORDER BY year, quarter 
