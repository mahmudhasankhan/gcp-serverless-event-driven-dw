-- Test: Every FK in the fact table must resolve to a dimension record
-- Orphaned keys mean a dimension was not populated correctly in Job 2
-- Any rows returned = test FAILS

SELECT
    f.sale_key,
    f.order_id,
    'customer_key orphaned'     AS issue_type
FROM {{ ref('fct_grocery_sales') }} f
LEFT JOIN {{ ref('dim_customer') }} d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL

UNION ALL

SELECT
    f.sale_key,
    f.order_id,
    'product_key orphaned'
FROM {{ ref('fct_grocery_sales') }} f
LEFT JOIN {{ ref('dim_product') }} d ON f.product_key = d.product_key
WHERE d.product_key IS NULL

UNION ALL

SELECT
    f.sale_key,
    f.order_id,
    'salesperson_key orphaned'
FROM {{ ref('fct_grocery_sales') }} f
LEFT JOIN {{ ref('dim_salesperson') }} d ON f.salesperson_key = d.sales_person_key
WHERE d.sales_person_key IS NULL

UNION ALL

SELECT
    f.sale_key,
    f.order_id,
    'shipper_key orphaned'
FROM {{ ref('fct_grocery_sales') }} f
LEFT JOIN {{ ref('dim_shipper') }} d ON f.shipper_key = d.shipper_key
WHERE d.shipper_key IS NULL

UNION ALL

SELECT
    f.sale_key,
    f.order_id,
    'region_key orphaned'
FROM {{ ref('fct_grocery_sales') }} f
LEFT JOIN {{ ref('dim_region') }} d ON f.region_key = d.region_key
WHERE d.region_key IS NULL

UNION ALL

SELECT
    f.sale_key,
    f.order_id,
    'payment_type_key orphaned'
FROM {{ ref('fct_grocery_sales') }} f
LEFT JOIN {{ ref('dim_payment_type') }} d ON f.payment_type_key = d.payment_type_key
WHERE d.payment_type_key IS NULL
