
WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

region_deduped AS (
    SELECT DISTINCT
        city,
        state,
        country,
        region 
    FROM sales 
),

final AS (

    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['city', 'state', 'country', 'region']) }} AS region_key,

        -- Descriptive Values
        city,
        state,
        country, 
        region 
    FROM region_deduped 
)

SELECT * FROM final