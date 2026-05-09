
WITH facts AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

product AS (
    SELECT * FROM {{ ref('dim_product') }}
),

product_aggregated AS (
    SELECT
        p.product_key,
        p.product_name,
        SUM(f.revenue) AS total_revenue,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sale_count) AS total_sales,
    FROM facts f 
    INNER JOIN product p
        ON f.product_key = p.product_key 
    GROUP BY p.product_key, p.product_name
),

product_ranked AS (

    SELECT
        pa.product_name,
        pa.total_revenue,
        pa.total_quantity,
        pa.total_sales,
        RANK() OVER(ORDER BY pa.total_revenue DESC) AS rank_by_revenue,
        RANK() OVER(ORDER BY pa.total_quantity DESC) AS rank_by_quantity,
        RANK() OVER(ORDER BY pa.total_sales DESC) AS rank_by_sales,
    FROM product_aggregated pa
)

SELECT * FROM product_ranked ORDER BY rank_by_revenue