
WITH facts AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

region AS (
    SELECT * FROM {{ ref('dim_region') }}
),

city_aggregated AS (
    SELECT
        r.city,
        SUM(f.revenue) AS total_revenue,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sale_count) AS total_sales,
    FROM facts f 
    INNER JOIN region r
        ON f.region_key = r.region_key
    GROUP BY r.city 
),

city_ranked AS (

    SELECT
        ca.city,
        ca.total_revenue,
        ca.total_quantity,
        ca.total_sales,
        RANK() OVER(ORDER BY ca.total_revenue DESC) AS rank_by_revenue,
        RANK() OVER(ORDER BY ca.total_quantity DESC) AS rank_by_quantity,
        RANK() OVER(ORDER BY ca.total_sales DESC) AS rank_by_sales,
    FROM city_aggregated ca
)

SELECT * FROM city_ranked ORDER BY rank_by_revenue