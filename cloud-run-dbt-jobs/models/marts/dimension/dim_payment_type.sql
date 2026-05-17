
WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

payment_deduped AS (
    SELECT DISTINCT 
    payment_type
    FROM sales 
),

final AS (

    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['payment_type']) }} AS payment_type_key,

        -- Descriptive Values
        payment_type 
    FROM payment_deduped 
)

SELECT * FROM final