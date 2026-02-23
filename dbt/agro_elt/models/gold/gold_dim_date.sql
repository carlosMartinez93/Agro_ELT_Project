-- models/gold/gold_dim_date.sql
-- Gold: date dimension derived from silver_weather_daily

with d as (
  select distinct date as date_day
  from {{ ref('silver_weather_daily') }}
)

select
  date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day,
  format_date('%Y-%m', date_day) as year_month
from d
