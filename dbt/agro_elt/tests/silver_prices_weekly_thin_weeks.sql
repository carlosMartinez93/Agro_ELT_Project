{{ config(severity='warn') }}

-- Warn when a week has too few stations for the average to be reliable.
select *
from {{ ref('silver_prices_weekly') }}
where n_stations < 20