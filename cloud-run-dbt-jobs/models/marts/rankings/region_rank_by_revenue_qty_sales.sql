WITH facts AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

region AS (
    SELECT * FROM {{ ref('dim_region') }}
),

region_aggregated AS (
    SELECT
        r.region,
        SUM(f.revenue) AS total_revenue,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sale_count) AS total_sales,
    FROM facts f 
    INNER JOIN region r
        ON f.region_key = r.region_key
    GROUP BY r.region 
),

region_ranked AS (

    SELECT
        ra.region,
        ra.total_revenue,
        ra.total_quantity,
        ra.total_sales,
        RANK() OVER(ORDER BY ra.total_revenue DESC) AS rank_by_revenue,
        RANK() OVER(ORDER BY ra.total_quantity DESC) AS rank_by_quantity,
        RANK() OVER(ORDER BY ra.total_sales DESC) AS rank_by_sales,
    FROM region_aggregated ra
)

SELECT * FROM region_ranked ORDER BY rank_by_revenue