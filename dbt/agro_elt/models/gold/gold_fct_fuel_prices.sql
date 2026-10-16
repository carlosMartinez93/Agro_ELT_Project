-- Gold: weekly fuel price fact
-- Grain: (week_start, state, product)

select
  week_start,
  state,
  product,
  avg_price,
  median_price,
  n_obs,
  n_stations
from {{ ref('silver_prices_weekly') }}