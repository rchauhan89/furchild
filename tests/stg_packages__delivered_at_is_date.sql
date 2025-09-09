{{ config(severity='warn') }}
select *
from {{ ref('stg_packages') }}
where delivered_at is not null
  and try_to_date(delivered_at) is null
