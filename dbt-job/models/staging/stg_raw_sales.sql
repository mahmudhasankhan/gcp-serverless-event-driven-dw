
WITH

source AS (
    SELECT * FROM {{ source('raw_data', 'sales') }}
),

transformed AS (
    SELECT 
        order_id,
        order_date,
        customer_id,
        TRIM(customer_name) AS customer_name,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(country_region) AS country,
        TRIM(salesperson) AS sales_person,
        TRIM(region) AS region,
        shipped_date,
        TRIM(shipper_name) AS shipper_name,
        TRIM(ship_name) AS ship_name,
        TRIM(ship_address) AS ship_address,
        TRIM(ship_city) AS ship_city,
        TRIM(ship_country_region) AS ship_country,
        TRIM(payment_type) AS payment_type,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        CAST(unit_price AS NUMERIC) AS unit_price,
        CAST(quantity AS INTEGER) AS quantity,
        ROUND(CAST(revenue AS NUMERIC)) AS revenue,
        ROUND(CAST(shipping_fee AS NUMERIC)) AS shipping_fee,
        ROUND(CAST(revenue_bins AS NUMERIC)) AS revenue_bins,
        batch_id,
        loaded_at
    FROM source
    
)

SELECT * FROM transformed
