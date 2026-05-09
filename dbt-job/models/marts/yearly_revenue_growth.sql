
WITH sales AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

yearly_revenue AS (
    SELECT
        d.year,
        SUM(s.revenue) AS total_revenue
    FROM sales s
    LEFT JOIN dates d ON s.order_date_key = d.date_key
    GROUP BY d.year
)
,
comparison AS (
    SELECT
        *,
        LAG(total_revenue) OVER (ORDER BY year) AS prev_year_revenue,
        ROUND(SAFE_DIVIDE(
            total_revenue - LAG(total_revenue) OVER (ORDER BY year),
            LAG(total_revenue) OVER (ORDER BY year)
        ) * 100, 2) AS yoy_growth_rate
    FROM yearly_revenue 
)

SELECT * FROM comparison 
ORDER BY year
