-- Test: Customer ID should always have the same name, city, state, country
-- Checks for data consistency in customer attributes

WITH customer_variations AS (
    SELECT
        customer_id,
        COUNT(DISTINCT customer_name) AS name_variations,
        COUNT(DISTINCT city) AS city_variations,
        COUNT(DISTINCT state) AS state_variations,
        COUNT(DISTINCT country) AS country_variations
    FROM {{ ref('stg_sales') }} 
    GROUP BY customer_id
)

SELECT
    customer_id,
    name_variations,
    city_variations,
    state_variations,
    country_variations,
FROM customer_variations
WHERE name_variations > 1 
   OR city_variations > 1 
   OR state_variations > 1 
   OR country_variations > 1