{{ config(materialized='table') }}

with first_order as (
  select
    customer_id,
    order_id,
    to_date(created_at_local)                        as order_dt,      -- CREATED_AT_LOCAL → DATE
    coalesce(device_type, 'unknown')                 as channel_dim,   -- use DEVICE_TYPE as channel
    row_number() over (
      partition by customer_id
      order by to_date(created_at_local), order_id
    ) as rn
  from {{ ref('stg_orders') }}
  where customer_id is not null
)

select
  fo.channel_dim                                     as channel,
  count(distinct c.customer_id)                      as customers,
  round(avg(c.ltv_revenue_12m), 0)                   as ltv12_avg,
  round(percentile_cont(0.50) within group (order by c.ltv_revenue_12m), 0) as ltv12_p50,
  round(avg(c.ltv_revenue_24m), 0)                   as ltv24_avg
from (select * from first_order where rn = 1) fo
join {{ ref('customer_revenue_ltv') }} c using (customer_id)
group by fo.channel_dim
order by ltv12_avg desc
