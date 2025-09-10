{{ config(
  materialized        = 'incremental',
  unique_key          = 'order_sk',
  on_schema_change    = 'append_new_columns'
) }}

-- ---------- Line-level source (already normalized to 38,6) ----------
with oi as (
  select
    cast(order_id as varchar) as order_id,
    created_at_local,
    created_at_utc,
    product_id,
    -- line-level amounts (38,6)
    cast(quantity            as decimal(38,6)) as quantity_386,
    cast(unit_price          as decimal(38,6)) as unit_price_386,
    cast(line_gross          as decimal(38,6)) as line_gross_386,
    cast(line_discount_amt   as decimal(38,6)) as line_discount_386,
    cast(line_tax_amt        as decimal(38,6)) as line_tax_386,
    cast(line_net_amt        as decimal(38,6)) as line_net_386,
    cast(discount_pct_effective as decimal(38,6)) as discount_pct_line_386,
    cast(vat_pct             as decimal(38,6)) as vat_pct_line_386
  from {{ ref('fct_order_items') }}
  {% if is_incremental() %}
    where _last_ingested_at >= dateadd(day, -7, current_timestamp())
  {% endif %}
),

-- ---------- Aggregate to order-level ----------
agg as (
  select
    order_id,
    -- timestamps: take MIN to be deterministic (all lines share the same in normal cases)
    min(created_at_local) as created_at_local,
    min(created_at_utc)   as created_at_utc,

    count(*)                                        as item_count,
    count(distinct product_id)                      as distinct_products,

    -- core amounts (stay 38,6)
    sum(coalesce(line_gross_386,    0::number(38,6))) as order_gross_386,
    sum(coalesce(line_discount_386, 0::number(38,6))) as order_discount_386,
    sum(coalesce(line_tax_386,      0::number(38,6))) as order_tax_386,
    sum(coalesce(line_net_386,      0::number(38,6))) as order_net_386,

    -- net before tax = net - tax  (avoid recomputing per-line)
    sum(coalesce(line_net_386, 0::number(38,6))) - sum(coalesce(line_tax_386, 0::number(38,6))) as order_net_bt_386
  from oi
  group by 1
),

-- ---------- Orders table: dates + VAT input (if needed) ----------
orders as (
  select
    cast(order_id as varchar) as order_id,
    created_at_local  as created_at_local_src,
    created_at_utc    as created_at_utc_src,
    cast(vat_percentage as decimal(38,6)) as vat_pct_src_386
  from {{ ref('stg_orders') }}
),

-- ---------- Join (ids are VARCHAR on both sides) ----------
joined as (
  select
    a.*,
    o.vat_pct_src_386
  from agg a
  left join orders o using (order_id)
),

-- ---------- Final amounts & derived percentages in a single domain ----------
finalized as (
  select
    j.*,

    -- effective discount % at order level: discount / gross * 100
    cast(
      case
        when j.order_gross_386 is null or j.order_gross_386 = 0::number(38,6) then 0::number(38,6)
        else round( (j.order_discount_386 / j.order_gross_386) * 100::number(38,6), 2)
      end
    as number(38,6)) as order_discount_pct_386,

    -- effective VAT % at order level (from math): tax / net_before_tax * 100
    cast(
      case
        when j.order_net_bt_386 is null or j.order_net_bt_386 = 0::number(38,6) then 0::number(38,6)
        else round( (j.order_tax_386 / j.order_net_bt_386) * 100::number(38,6), 2)
      end
    as number(38,6)) as order_vat_pct_eff_386
  from joined j
)

select
  -- SK
  md5(cast(coalesce(cast(order_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as order_sk,

  -- keys
  order_id,

  -- timestamps
  created_at_local,
  created_at_utc,
  to_date(created_at_local) as order_date_local,

  -- counts
  item_count,
  distinct_products,

  -- amounts (rounded for display; still 38,6)
  round(order_gross_386,    2) as order_gross,
  round(order_discount_386, 2) as order_discount_amt,
  round(order_net_bt_386,   2) as order_net_before_tax,
  round(order_tax_386,      2) as order_tax_amt,
  round(order_net_386,      2) as order_net_amt,

  -- percentages (0..100, rounded; still 38,6)
  round(order_discount_pct_386, 2) as order_discount_pct_effective,
  round(order_vat_pct_eff_386,  2) as order_vat_pct_effective,

  -- optional: the source VAT % recorded on the order (may differ from effective)
  round(vat_pct_src_386, 2) as vat_pct_src,

  -- audit
  current_timestamp() as _calculated_at
from finalized
{% if is_incremental() %}
where _calculated_at >= dateadd(day, -7, current_timestamp())
{% endif %}
