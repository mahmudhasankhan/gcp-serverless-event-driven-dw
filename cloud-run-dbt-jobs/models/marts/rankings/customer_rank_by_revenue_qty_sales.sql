WITH facts AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

customer_aggregated AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.customer_name,
        SUM(f.revenue) AS total_revenue,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sale_count) AS total_sales,
    FROM facts f 
    INNER JOIN customer c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key, c.customer_id, c.customer_name
),

customer_ranked AS (

    SELECT
        ca.customer_name,
        ca.total_revenue,
        ca.total_quantity,
        ca.total_sales,
        RANK() OVER(ORDER BY total_revenue DESC) AS rank_by_revenue,
        RANK() OVER(ORDER BY total_quantity DESC) AS rank_by_quantity,
        RANK() OVER(ORDER BY total_sales DESC) AS rank_by_sales,
    FROM customer_aggregated ca
)

SELECT * FROM customer_ranked ORDER BY rank_by_revenue