
WITH facts AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

salesperson AS (
    SELECT * FROM {{ ref('dim_salesperson') }}
),

salesperson_aggregated AS (
    SELECT
        s.sales_person_key,
        s.sales_person,
        SUM(f.revenue) AS total_revenue,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sale_count) AS total_sales,
    FROM facts f 
    INNER JOIN salesperson s
        ON f.salesperson_key = s.sales_person_key 
    GROUP BY s.sales_person_key, s.sales_person 
),

salesperson_ranked AS (

    SELECT
        sa.sales_person,
        sa.total_revenue,
        sa.total_quantity,
        sa.total_sales,
        RANK() OVER(ORDER BY sa.total_revenue DESC) AS rank_by_revenue,
        RANK() OVER(ORDER BY sa.total_quantity DESC) AS rank_by_quantity,
        RANK() OVER(ORDER BY sa.total_sales DESC) AS rank_by_sales,
    FROM salesperson_aggregated sa
)

SELECT * FROM salesperson_ranked ORDER BY rank_by_revenue