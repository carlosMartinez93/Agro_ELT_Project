-- Gold: location dimension
select
  location_key,
  city,
  state,
  region,
  latitude,
  longitude
from {{ ref('bronze_locations') }}