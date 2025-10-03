{{ config(materialized='table') }}

with c as (
  select
    customer_id,
    date_trunc('month', first_order_date)::date as cohort_month,
    ltv_revenue_6m, ltv_revenue_12m, ltv_revenue_24m
  from {{ ref('customer_revenue_ltv') }}
)

select
  cohort_month,
  count(distinct customer_id)                                   as customers,
  avg(ltv_revenue_6m)  as avg_ltv_6m,
  avg(ltv_revenue_12m) as avg_ltv_12m,
  avg(ltv_revenue_24m) as avg_ltv_24m,
  percentile_cont(0.50) within group (order by ltv_revenue_12m) as p50_ltv_12m,
  percentile_cont(0.75) within group (order by ltv_revenue_12m) as p75_ltv_12m
from c
group by 1
order by 1
