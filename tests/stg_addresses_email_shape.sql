{{ config(severity='warn', store_failures=false) }}
select *
from {{ ref('stg_addresses') }}
where email is not null
  and position('@' in email) <= 1
