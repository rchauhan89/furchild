{{ config(
  materialized = 'table',
  unique_key   = 'customer_id'
) }}

with base as (
  select
    d.customer_id,
    d.is_first_order,
    o.order_id,
    o.order_date_local,
    o.order_net_amt
  from {{ ref('fct_orders') }} o
  join {{ ref('dim_orders') }} d
    on o.order_sk = d.order_sk
  where d.customer_id is not null
),

agg as (
  select
    customer_id,

    -- Core KPIs
    count(distinct order_id)                         as order_count,
    sum(coalesce(order_net_amt, 0))                  as ltv,
    avg(coalesce(order_net_amt, 0))                  as avg_order_value,

    -- Recency
    max(order_date_local)                            as last_order_date,
    datediff(day, max(order_date_local), current_date) as days_since_last_order,

    -- First order
    min(order_date_local)                            as first_order_date
  from base
  group by customer_id
)

select
  customer_id,
  order_count          as frequency,
  ltv,
  avg_order_value,
  first_order_date,
  last_order_date,
  days_since_last_order
from agg

