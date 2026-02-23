-- Gold: weather fact table
-- Grain: (date_day, location_key)

select
  w.date as date_day,
  w.location_key,
  w.temp_max,
  w.temp_min,
  w.precip_sum
from {{ ref('silver_weather_daily') }} w
