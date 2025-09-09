{{ config(severity='warn', store_failures=false) }}
select *
from {{ ref('stg_addresses') }}
where (latitude  is not null and not (latitude  between -90  and 90))
   or (longitude is not null and not (longitude between -180 and 180))
