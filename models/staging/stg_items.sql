{{ config(materialized='table', transient=true) }}
-- If your PRICE is already ex-VAT, set vat_rate to 0.0 in dbt_project.yml
-- vars: { vat_rate: 0.05 }

select
  t.transaction_item_id,
  t.transaction_id,
  t.product_id,
  t.product_name,
  t.stock_id,
  t.stock_name,
  t.unit,
  t.quantity,
  t.unit_price_ex_vat,
  t.total_discount_pct,
  t.unit_price_ex_vat * (1 - t.total_discount_pct) as realized_unit_price_ex_vat
from (
  select
    cast(id as varchar)                 as transaction_item_id,
    cast(trim(transaction_id) as varchar)     as transaction_id,

    cast(product_id as varchar)         as product_id,
    product_name,
    cast(stock_id as varchar)           as stock_id,
    stock_name,
    unit,
    quantity,

    case
      when has_vat = 1 then price / (1 + {{ var('vat_rate', 0.05) }})
      else price
    end as unit_price_ex_vat,

    least(
      1.0,
      greatest(
        0.0,
        (coalesce(discount_percentage, 0)
         + coalesce(bulk_discount_percentage, 0)
         + coalesce(friendbuy_discount_percentage, 0)) / 100.0
      )
    ) as total_discount_pct
  from {{ source('bronze','transaction_items') }}
) t
