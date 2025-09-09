{{ config(severity='warn') }}

select *
from {{ ref('stg_shipments') }}
where delivered_at is not null
  and shipped_at is not null
  and delivered_at < shipped_at
