WITH sales AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        d.year,
        d.quarter,
        d.quarter_name,
        d.month,
        d.month_name,
        SUM(s.revenue) AS total_revenue,
        SUM(s.quantity) AS total_quantity,
        SUM(s.sale_count) AS total_sales

    FROM sales s
    LEFT JOIN dates d ON s.order_date_key = d.date_key
    GROUP BY d.year, d.quarter, d.quarter_name, d.month, d.month_name
    ORDER BY d.year, d.quarter, d.month
)

SELECT * FROM final ORDER BY year, quarter, quarter_name, month