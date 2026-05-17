WITH date_spine AS (
    
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    )
    }}
),

date_dimension AS (
    SELECT

        -- Surrogate Key

        {{ dbt_utils.generate_surrogate_key(['CAST(date_day AS DATE)']) }} AS date_key,
        
        -- Actual date
        CAST(date_day AS DATE) AS date,
        
        -- Year attributes
        EXTRACT(YEAR FROM date_day) AS year,
        
        -- Quarter attributes
        EXTRACT(QUARTER FROM date_day) AS quarter,
        CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_day) AS STRING), ' ', CAST(EXTRACT(YEAR FROM date_day) AS STRING)) AS quarter_name,
        
        -- Month attributes
        EXTRACT(MONTH FROM date_day) AS month,
        FORMAT_DATE('%B', date_day) AS month_name,
        FORMAT_DATE('%b', date_day) AS month_name_short,
        
        -- Week attributes
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(ISOWEEK FROM date_day) AS iso_week,
        
        -- Day attributes
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
        FORMAT_DATE('%A', date_day) AS day_name,
        FORMAT_DATE('%a', date_day) AS day_name_short,
        EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,
        
        -- Boolean flags
        CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) BETWEEN 2 AND 6 THEN TRUE ELSE FALSE END AS is_weekday,
        
        -- Fiscal calendar (assuming fiscal year starts in January, adjust as needed)
        EXTRACT(YEAR FROM date_day) AS fiscal_year,
        EXTRACT(QUARTER FROM date_day) AS fiscal_quarter,
        
        -- Relative date calculations (useful for YTD, MTD, QTD)
        CAST(DATE_TRUNC(date_day, YEAR) AS DATE) AS year_start_date,
        CAST(DATE_TRUNC(date_day, QUARTER) AS DATE) AS quarter_start_date,
        CAST(DATE_TRUNC(date_day, MONTH) AS DATE) AS month_start_date,
        CAST(DATE_TRUNC(date_day, WEEK) AS DATE) AS week_start_date,
    
    FROM date_spine 
)

SELECT * from date_dimension