{{ config(materialized='table') }}

with items as (
  select
    transaction_id,
    sum(realized_unit_price_ex_vat * quantity) as order_revenue_ex_vat
  from {{ ref('stg_items') }}
  group by 1
),

orders as (
  select
    cast(id as varchar)                            as order_id,
    to_timestamp_tz(transaction_date)              as order_ts,
    to_date(transaction_date)                      as order_date,
    upper(coalesce(payment_status,''))             as payment_status,
    upper(coalesce(delivery_status,''))            as delivery_status,
    lower(coalesce(device_type,'unknown'))         as channel,
    cast(user_id as varchar)                       as customer_id
  from {{ source('bronze','transactions') }}
)

select
  o.customer_id,
  o.order_id,
  o.order_ts,
  o.order_date,
  o.channel,
  i.order_revenue_ex_vat
from orders o
join items i
  on i.transaction_id = o.order_id
where o.payment_status in ('PAID','CAPTURED','SETTLED')
  and o.delivery_status not in ('CANCELLED','REFUNDED','FAILED')
  and o.customer_id is not null
  and o.order_date is not null

