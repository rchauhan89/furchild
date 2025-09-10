{{ config(
  materialized        = 'incremental',
  unique_key          = 'order_item_sk',
  on_schema_change    = 'append_new_columns'
) }}

with const as (
  select
    cast(0   as decimal(38,6)) as z386,
    cast(100 as decimal(38,6)) as c100_386
),

/* ---------- ITEMS: IDs as VARCHAR; numerics via CAST (no try_to_number) ---------- */
items as (
  select
    cast(ORDER_ITEM_ID as varchar)  as order_item_id,
    cast(ORDER_ID      as varchar)  as order_id,
    cast(PRODUCT_ID    as varchar)  as product_id,
    PRODUCT_NAME                     as product_name,
    cast(STOCK_ID      as varchar)  as stock_id,
    STOCK_NAME                       as stock_name,
    UNIT                             as unit,

    cast(QUANTITY     as decimal(38,6)) as quantity_raw,
    cast(PRICE        as decimal(38,6)) as unit_price_raw,
    cast(LINE_AMOUNT  as decimal(38,6)) as line_amount_raw,

    HAS_VAT                              as has_vat_raw,

    cast(DISCOUNT_PERCENTAGE             as decimal(38,6)) as discount_pct_in_raw,
    cast(BULK_DISCOUNT_PERCENTAGE        as decimal(38,6)) as bulk_discount_pct_in_raw,
    upper(coalesce(DISCOUNT_APPLICATION_TYPE,'PERCENTAGE')) as discount_application_type,

    cast(FRIENDBUY_DISCOUNT_PERCENTAGE   as decimal(38,6)) as friendbuy_discount_pct_in_raw,
    upper(coalesce(FRIENDBUY_DISCOUNT_APPLICATION_TYPE,'PERCENTAGE')) as friendbuy_discount_application_type,

    MEAL_PLAN_REQUEST_ID as meal_plan_request_id,
    _INGESTED_AT         as _ingested_at
  from {{ ref('stg_order_items') }}
  {% if is_incremental() %}
    where _INGESTED_AT >= dateadd(day, -7, current_timestamp())
  {% endif %}
),

/* ---------- ORDERS: join key as VARCHAR; VAT numeric via CAST ---------- */
orders as (
  select
    cast(order_id as varchar) as order_id,
    created_at_local,
    created_at_utc,
    cast(vat_percentage as decimal(38,6)) as vat_pct_raw
  from {{ ref('stg_orders') }}
),

/* ---------- Normalize all numerics to one domain (38,6) ---------- */
typed as (
  select
    i.*,
    c.z386, c.c100_386,

    case when i.has_vat_raw in (1,'1','true','TRUE') then true else false end as has_vat,

    coalesce(cast(i.quantity_raw    as decimal(38,6)), c.z386) as qty_386,
    coalesce(cast(i.unit_price_raw  as decimal(38,6)), c.z386) as price_386,
    coalesce(cast(i.line_amount_raw as decimal(38,6)), c.z386) as line_amount_src_386,

    coalesce(cast(i.discount_pct_in_raw           as decimal(38,6)), c.z386) as disc_in_386,
    coalesce(cast(i.friendbuy_discount_pct_in_raw as decimal(38,6)), c.z386) as friendbuy_in_386,
    coalesce(cast(i.bulk_discount_pct_in_raw      as decimal(38,6)), c.z386) as bulk_in_386,

    coalesce(cast(o.vat_pct_raw as decimal(38,6)), c.z386) as vat_pct_386,

    o.created_at_local,
    o.created_at_utc
  from items i
  cross join const c
  left join orders o using (order_id)
),

/* ---------- Accept percentage inputs only when type is PERCENTAGE ---------- */
pct as (
  select
    t.*,
    case when t.discount_application_type = 'PERCENTAGE' then t.disc_in_386      else t.z386 end as disc_pct_386,
    case when t.friendbuy_discount_application_type = 'PERCENTAGE' then t.friendbuy_in_386 else t.z386 end as friendbuy_pct_386,
    t.bulk_in_386 as bulk_pct_386
  from typed t
),

/* ---------- Compute amounts in one numeric domain (38,6) ---------- */
amt as (
  select
    p.*,

    cast(p.qty_386 * p.price_386 as decimal(38,6)) as line_gross_386,

    cast(
      case
        when coalesce(p.disc_pct_386, p.z386) + coalesce(p.friendbuy_pct_386, p.z386) + coalesce(p.bulk_pct_386, p.z386) < p.z386
          then p.z386
        when coalesce(p.disc_pct_386, p.z386) + coalesce(p.friendbuy_pct_386, p.z386) + coalesce(p.bulk_pct_386, p.z386) > p.c100_386
          then p.c100_386
        else round(coalesce(p.disc_pct_386, p.z386) + coalesce(p.friendbuy_pct_386, p.z386) + coalesce(p.bulk_pct_386, p.z386), 2)
      end
    as decimal(38,6)) as disc_pct_eff_386,

    cast( case when p.vat_pct_386 is null then p.z386 else p.vat_pct_386 / p.c100_386 end as decimal(38,6)) as vat_frac_386
  from pct p
),

/* ---------- Step 1: compute fraction + net-before-tax ---------- */
math1 as (
  select
    a.*,
    cast(a.disc_pct_eff_386 / a.c100_386 as decimal(38,6)) as disc_frac_386,
    cast(a.line_gross_386 - (a.line_gross_386 * (a.disc_pct_eff_386 / a.c100_386)) as decimal(38,6)) as line_net_bt_386,
    cast(a.line_gross_386 * (a.disc_pct_eff_386 / a.c100_386) as decimal(38,6))   as line_discount_386
  from amt a
),

/* ---------- Step 2: compute tax from prior alias ---------- */
math as (
  select
    m1.*,
    cast(case when m1.has_vat then m1.line_net_bt_386 * m1.vat_frac_386 else m1.z386 end as decimal(38,6)) as line_tax_386
  from math1 m1
)

select
  /* SKs as strings to avoid macro-side typing differences */
  md5(cast(coalesce(cast(order_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
           coalesce(cast(product_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
           coalesce(cast(order_item_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as order_item_sk,
  md5(cast(coalesce(cast(order_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as order_sk,

  order_item_id,
  order_id,
  product_id,
  product_name,
  stock_id,
  stock_name,

  created_at_local,
  created_at_utc,
  to_date(created_at_local) as order_date_local,

  /* keep numeric outputs in 38,6 (rounded), no 18,2 anywhere */
  unit,
  round(qty_386,   2) as quantity,
  round(price_386, 2) as unit_price,

  round(line_amount_src_386, 2) as line_amount_raw,
  round(line_gross_386,     2) as line_gross,

  round(disc_pct_eff_386, 2) as discount_pct_effective,  -- percent 0..100
  round(line_discount_386, 2) as line_discount_amt,

  round(vat_pct_386, 2) as vat_pct,                      -- percent 0..100
  round(line_tax_386, 2) as line_tax_amt,
  round(line_net_bt_386 + line_tax_386, 2) as line_net_amt,

  meal_plan_request_id,
  _INGESTED_AT as _last_ingested_at

from math
{% if is_incremental() %}
where _last_ingested_at >= dateadd(day, -7, current_timestamp())
{% endif %}
