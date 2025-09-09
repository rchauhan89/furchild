{{ config(severity='warn') }}

select *
from {{ ref('stg_shipments') }}
where shipped_at is not null
  and try_to_date(shipped_at) is null