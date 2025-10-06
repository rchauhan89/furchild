{{ config(materialized='table') }}

with o as (
  select customer_id, order_date, order_revenue_ex_vat
  from {{ ref('fct_orders_revenue') }}
),
firsts as (
  select customer_id, min(order_date) as first_order_date
  from o
  group by 1
),
by_month as (
  select
    customer_id,
    date_trunc('month', order_date)::date as month_start,
    sum(order_revenue_ex_vat)            as month_revenue
  from o
  group by 1,2
)
select
  b.customer_id,
  f.first_order_date,
  b.month_start,
  datediff(
    month,
    date_trunc('month', f.first_order_date),
    b.month_start
  ) as months_since_acq,
  b.month_revenue
from by_month b
join firsts f using (customer_id)
where datediff(month, date_trunc('month', f.first_order_date), b.month_start) >= 0
