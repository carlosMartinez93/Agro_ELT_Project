-- Bronze layer DDL (dataset: analytics_engineering; edit the name below to use another one)
-- Fuel prices have no bronze table: they are read directly from the Base dos Dados mirror (see dbt sources).
-- bronze_locations is a dbt seed (see seeds/bronze_locations.csv).

create table if not exists analytics_engineering.bronze_weather_daily (
  date        date      not null,
  location_key string   not null,
  latitude    float64,
  longitude   float64,
  temp_max    float64,
  temp_min    float64,
  precip_sum  float64,
  source      string,
  ingested_at timestamp
)
cluster by location_key;

create table if not exists analytics_engineering.ingestion_state (
  source           string not null,
  entity           string not null,
  last_loaded_date date,
  updated_at       timestamp
);