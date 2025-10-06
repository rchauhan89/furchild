{{ config(materialized='table') }}

with items as (
  select
    transaction_id,
    sum(realized_unit_price_ex_vat * quantity) as order_revenue_ex_vat
  from {{ ref('stg_items') }}
  group by 1
)

select
  o.customer_id,
  o.order_id,
  -- CREATED_AT_LOCAL is TIMESTAMP_NTZ → convert to DATE without double-casting
  to_date(o.created_at_local) as order_date,
  -- keep it simple (avoid compile errors if channel column varies)
  'unknown' as channel,
  i.order_revenue_ex_vat
from {{ ref('stg_orders') }} o
join items i
  on i.transaction_id = o.order_id
where o.customer_id is not null
  and o.created_at_local is not null
