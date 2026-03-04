-- Fail if there are location_key values in the fact table that do not exist in the dimension.
select
  f.location_key
from {{ ref('gold_fct_weather_daily') }} f
left join {{ ref('gold_dim_location') }} d
  on f.location_key = d.location_key
where d.location_key is null
limit 1