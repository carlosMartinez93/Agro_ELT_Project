-- models/silver/silver_weather_daily.sql
-- Silver: cleaned, typed, and deduplicated daily weather
-- Grain: 1 row per (date, location_key)

with src as (
  select
    cast(date as date) as date,
    cast(location_key as string) as location_key,
    cast(latitude as float64) as latitude,
    cast(longitude as float64) as longitude,
    cast(temp_max as float64) as temp_max,
    cast(temp_min as float64) as temp_min,
    cast(precip_sum as float64) as precip_sum,
    cast(source as string) as source,
    cast(ingested_at as timestamp) as ingested_at
  from {{ source('bronze', 'bronze_weather_daily') }}
),

dedup as (
  select
    *,
    row_number() over (
      partition by date, location_key
      order by ingested_at desc
    ) as rn
  from src
)

select
  date, location_key, latitude, longitude,
  temp_max, temp_min, precip_sum,
  source, ingested_at
from dedup
where rn = 1
