{{ config(materialized='table') }}

with m as (select * from {{ ref('fct_customer_monthly_revenue') }}),

roll as (
  select
    customer_id,
    first_order_date,
    sum(case when months_since_acq between 0 and 5  then month_revenue end)  as ltv_revenue_6m,
    sum(case when months_since_acq between 0 and 11 then month_revenue end)  as ltv_revenue_12m,
    sum(case when months_since_acq between 0 and 23 then month_revenue end)  as ltv_revenue_24m
  from m
  group by 1,2
),

orders_cnt as (
  select customer_id, count(*) as orders_count
  from {{ ref('fct_orders_revenue') }}
  group by 1
),

rev_to_date as (
  select customer_id, sum(order_revenue_ex_vat) as revenue_to_date
  from {{ ref('fct_orders_revenue') }}
  group by 1
)

select
  r.customer_id,
  r.first_order_date,
  coalesce(o.orders_count, 0)    as orders_count,
  coalesce(rt.revenue_to_date,0) as revenue_to_date,
  coalesce(r.ltv_revenue_6m,  0) as ltv_revenue_6m,
  coalesce(r.ltv_revenue_12m, 0) as ltv_revenue_12m,
  coalesce(r.ltv_revenue_24m, 0) as ltv_revenue_24m
from roll r
left join orders_cnt o on o.customer_id = r.customer_id
left join rev_to_date rt on rt.customer_id = r.customer_id

