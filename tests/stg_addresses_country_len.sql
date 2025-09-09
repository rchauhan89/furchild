{{ config(severity='warn', store_failures=false) }}
select *
from {{ ref('stg_addresses') }}
where country_code is not null
  and length(country_code) not in (2,3)
