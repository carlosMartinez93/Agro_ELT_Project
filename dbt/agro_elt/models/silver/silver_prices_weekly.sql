{{ config(materialized='table') }}

-- Silver: weekly fuel prices by state and product, aggregated from station-level survey rows.
select
  date_trunc(data_coleta, week(monday)) as week_start,
  sigla_uf as state,
  case produto when 'Etanol' then 'ethanol' when 'Gasolina' then 'gasoline' end as product,
  avg(preco_venda) as avg_price,
  approx_quantiles(preco_venda, 2)[offset(1)] as median_price,
  count(*) as n_obs,
  count(distinct cnpj_revenda) as n_stations
from {{ source('basedosdados', 'microdados') }}
where ano >= 2022
  and sigla_uf in ('SP', 'GO', 'MG', 'MS', 'PR')
  and produto in ('Etanol', 'Gasolina')
  and preco_venda between 1 and 20
group by 1, 2, 3