-- Gold: weather x fuel price, one row per price week and rainfall lag
-- Grain: (week_start, state, product, lag_weeks)
-- weather_week_start = week_start - lag_weeks weeks. Weather comes from one representative city per state.

with weather_weekly as (
  select
    date_trunc(f.date_day, week(monday)) as week_start,
    upper(l.state) as state,
    sum(f.precip_sum) as precip_mm,
    avg(f.temp_max) as temp_max_avg
  from {{ ref('gold_fct_weather_daily') }} f
  join {{ ref('gold_dim_location') }} l using (location_key)
  group by 1, 2
  having count(*) = 7  -- only complete weeks
),

prices as (
  select
    week_start,
    state,
    product,
    avg_price,
    n_stations,
    case
      when date_diff(week_start, lag(week_start) over w, day) = 7
        then avg_price / lag(avg_price) over w - 1
    end as price_change_pct
  from {{ ref('gold_fct_fuel_prices') }}
  window w as (partition by state, product order by week_start)
)

select
  p.week_start,
  p.state,
  p.product,
  lag_weeks,
  date_sub(p.week_start, interval lag_weeks * 7 day) as weather_week_start,
  p.avg_price,
  p.price_change_pct,
  p.n_stations,
  w.precip_mm,
  w.temp_max_avg
from prices p
cross join unnest(generate_array(0, 8)) as lag_weeks
join weather_weekly w
  on w.state = p.state
 and w.week_start = date_sub(p.week_start, interval lag_weeks * 7 day)