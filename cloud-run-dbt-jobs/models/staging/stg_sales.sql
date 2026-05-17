/*
    stg_sales
    ─────────
    Reads from raw_data.sales, casts and trims all columns,
    and returns only rows that pass quality checks.

    Bad rows are quarantined separately via:
        dbt run-operation quarantine_bad_rows
    which runs BEFORE this model in the Dockerfile.staging CMD.

    Quality rules — a row is excluded if ANY of these are true:
      - order_id or customer_id is null
      - unit_price is null or <= 0
      - quantity is null or <= 0
      - revenue is null or < 0
      - shipped_date < order_date
*/

{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'sales') }}
),

casted AS (
    SELECT
        order_id,
        order_date,
        customer_id,
        TRIM(customer_name)                     AS customer_name,
        TRIM(city)                              AS city,
        TRIM(state)                             AS state,
        TRIM(country_region)                    AS country,
        TRIM(salesperson)                       AS sales_person,
        TRIM(region)                            AS region,
        shipped_date,
        TRIM(shipper_name)                      AS shipper_name,
        TRIM(ship_name)                         AS ship_name,
        TRIM(ship_address)                      AS ship_address,
        TRIM(ship_city)                         AS ship_city,
        TRIM(ship_country_region)               AS ship_country,
        TRIM(payment_type)                      AS payment_type,
        TRIM(product_name)                      AS product_name,
        TRIM(category)                          AS category,
        CAST(unit_price  AS NUMERIC)            AS unit_price,
        CAST(quantity    AS INTEGER)            AS quantity,
        ROUND(CAST(revenue      AS NUMERIC), 2) AS revenue,
        ROUND(CAST(shipping_fee AS NUMERIC), 2) AS shipping_fee,
        ROUND(CAST(revenue_bins AS NUMERIC), 2) AS revenue_bins,
        batch_id,
        loaded_at
    FROM source
),

good_data AS (
    SELECT *
    FROM casted
    WHERE order_id      IS NOT NULL
      AND customer_id   IS NOT NULL
      AND unit_price    IS NOT NULL AND unit_price  > 0
      AND quantity      IS NOT NULL AND quantity    > 0
      AND revenue       IS NOT NULL AND revenue     >= 0
      AND (shipped_date IS NULL OR shipped_date >= order_date)
)

SELECT * FROM good_data