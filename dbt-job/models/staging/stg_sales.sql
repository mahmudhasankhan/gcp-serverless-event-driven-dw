WITH raw_data AS (
    SELECT * FROM {{ ref('stg_raw_sales') }}
),

cleaned_data AS (
    SELECT
        *
    FROM raw_data
    WHERE shipped_date >= order_date
    AND quantity IS NOT NULL
    AND unit_price IS NOT NULL
)

SELECT * FROM cleaned_data