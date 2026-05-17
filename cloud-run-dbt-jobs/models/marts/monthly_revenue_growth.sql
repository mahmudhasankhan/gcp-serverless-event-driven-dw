
WITH sales AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        SUM(s.revenue) AS total_revenue
    FROM sales s
    LEFT JOIN dates d ON s.order_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
),

comparison AS (
    SELECT
        *,
        LAG(total_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
        ROUND(SAFE_DIVIDE(
            total_revenue - LAG(total_revenue) OVER (ORDER BY year, month),
            LAG(total_revenue) OVER (ORDER BY year, month)
        ) * 100, 2) AS mom_growth_rate
    FROM monthly_revenue
)

SELECT * FROM comparison
ORDER BY year, month
