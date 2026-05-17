
WITH sales AS (
    SELECT * FROM {{ ref('fct_grocery_sales') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

aggregated AS (
    SELECT
        d.date,
        d.year,
        d.quarter,
        d.month,
        SUM(s.revenue) AS daily_revenue,
        SUM(s.quantity) AS daily_quantity,
        SUM(s.shipping_fee) AS daily_shipping_fee

    FROM sales s
    LEFT JOIN dates d ON s.order_date_key = d.date_key
    GROUP BY d.date, d.year, d.quarter, d.month 
),

cumulative_growth AS (
    SELECT
        *,
       
        -- ytd
        SUM(daily_revenue) OVER (PARTITION BY year ORDER BY date) AS ytd_revenue,
        SUM(daily_quantity) OVER (PARTITION BY year ORDER BY date) AS ytd_quantity,
        SUM(daily_shipping_fee) OVER (PARTITION BY year ORDER BY date) AS ytd_shipping_fee,
        
        {# -- qtd
        SUM(daily_revenue) OVER (PARTITION BY year, quarter ORDER BY date) AS qtd_revenue,
        SUM(daily_quantity) OVER (PARTITION BY year, quarter ORDER BY date) AS qtd_quantity,
        SUM(daily_shipping_fee) OVER (PARTITION BY year, quarter ORDER BY date) AS qtd_shipping_fee, #}
        
        {# -- mtd 
        SUM(daily_revenue) OVER (PARTITION BY year, month ORDER BY date) AS mtd_revenue,
        SUM(daily_quantity) OVER (PARTITION BY year, month ORDER BY date) AS mtd_quantity,
        SUM(daily_shipping_fee) OVER (PARTITION BY year, month ORDER BY date) AS mtd_shipping_fee, #}

        -- Growth rates (compared to previous day)
        -- revenue 
        SAFE_DIVIDE(
            daily_revenue - LAG(daily_revenue) OVER (ORDER BY date),
            LAG(daily_revenue) OVER (ORDER BY date)
        ) AS revenue_growth_rate,
        
        -- quantity 
        SAFE_DIVIDE(
            daily_quantity - LAG(daily_quantity) OVER (ORDER BY date),
            LAG(daily_quantity) OVER (ORDER BY date)
        ) AS quantity_growth_rate,
        
        -- shipping fee
        SAFE_DIVIDE(
            daily_shipping_fee - LAG(daily_shipping_fee) OVER (ORDER BY date),
            LAG(daily_shipping_fee) OVER (ORDER BY date)
        ) AS shipping_growth_rate

    FROM aggregated
)

SELECT * FROM cumulative_growth 
ORDER BY date, year, quarter, month    

