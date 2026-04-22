{{ config(materialized='table') }}

WITH base_data AS (
    SELECT * FROM {{ source('snowflake_raw', 'weather_historical') }}
)
SELECT 
    location_name,
    date,
    temp_mean,
    --  Seven-Day Moving Averages (Requirement: Moving Average)
    AVG(temp_mean) OVER (
        PARTITION BY location_name 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d,
    -- cumulative rainfall (Requirement: Rolling Rainfall)
    SUM(precipitation) OVER (
        PARTITION BY location_name 
        ORDER BY date
    ) as cumulative_rainfall
FROM base_data