-- Gold: correlation between weekly rainfall and weekly price change
-- Grain: (state, product, lag_weeks)
-- Two versions: raw, and seasonally adjusted (each week minus the mean of the same ISO week across years).
-- The t-statistics are naive: they ignore autocorrelation of weekly series, so they overstate significance.
-- The 3.46 threshold is a Bonferroni correction for 90 simultaneous tests (alpha 0.05 / 90).

with base as (
  select
    state,
    product,
    lag_weeks,
    week_start,
    weather_week_start,
    price_change_pct,
    precip_mm
  from {{ ref('gold_fct_weather_price') }}
  where price_change_pct is not null
),

adjusted as (
  select
    *,
    price_change_pct - avg(price_change_pct) over (
      partition by state, product, lag_weeks, extract(isoweek from week_start)
    ) as price_change_adj,
    precip_mm - avg(precip_mm) over (
      partition by state, product, lag_weeks, extract(isoweek from weather_week_start)
    ) as precip_adj
  from base
),

stats as (
  select
    state,
    product,
    lag_weeks,
    count(*) as n_weeks,
    corr(price_change_pct, precip_mm) as corr_raw,
    corr(price_change_adj, precip_adj) as corr_seasonally_adjusted
  from adjusted
  group by 1, 2, 3
),

with_t as (
  select
    *,
    corr_raw * sqrt((n_weeks - 2) / (1 - pow(corr_raw, 2))) as t_stat_raw,
    corr_seasonally_adjusted * sqrt((n_weeks - 2) / (1 - pow(corr_seasonally_adjusted, 2))) as t_stat_adjusted
  from stats
)

select
  *,
  abs(t_stat_raw) > 3.46 as significant_raw,
  abs(t_stat_adjusted) > 3.46 as significant_adjusted
from with_t