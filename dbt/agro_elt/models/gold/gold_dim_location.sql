-- Gold: location dimension
select
  location_key,
  city,
  state,
  region,
  latitude,
  longitude
from {{ source('bronze', 'bronze_locations') }}
