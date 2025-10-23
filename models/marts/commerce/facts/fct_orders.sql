{{ config(
  materialized        = 'incremental',
  unique_key          = 'order_sk',
  on_schema_change    = 'append_new_columns'
) }}

-- ---------- Line-level source (38,6) ----------
with oi as (
  select
    cast(order_id as varchar) as order_id,
    created_at_local,
    created_at_utc,
    product_id,
    cast(quantity               as number(38,6)) as quantity_386,
    cast(unit_price             as number(38,6)) as unit_price_386,
    cast(line_gross             as number(38,6)) as line_gross_386,
    cast(line_discount_amt      as number(38,6)) as line_discount_386,
    cast(line_tax_amt           as number(38,6)) as line_tax_386,
    cast(line_net_amt           as number(38,6)) as line_net_386,
    cast(discount_pct_effective as number(38,6)) as discount_pct_line_386,
    cast(vat_pct                as number(38,6)) as vat_pct_line_386
  from {{ ref('fct_order_items') }}
  {% if is_incremental() %}
    where _last_ingested_at >= dateadd(day, -7, current_timestamp())
  {% endif %}
),

-- ---------- Aggregate to order-level ----------
agg as (
  select
    order_id,
    min(created_at_local) as created_at_local,
    min(created_at_utc)   as created_at_utc,
    count(*)                   as item_count,
    count(distinct product_id) as distinct_products,
    sum(coalesce(line_gross_386,    0::number(38,6))) as order_gross_386,
    sum(coalesce(line_discount_386, 0::number(38,6))) as order_discount_386,
    sum(coalesce(line_tax_386,      0::number(38,6))) as order_tax_386,
    sum(coalesce(line_net_386,      0::number(38,6))) as order_net_386,
    sum(coalesce(line_net_386, 0::number(38,6))) - sum(coalesce(line_tax_386, 0::number(38,6))) as order_net_bt_386
  from oi
  group by 1
),

-- ---------- Orders table: VAT input ----------
orders as (
  select
    cast(order_id as varchar)            as order_id,
    created_at_local                     as created_at_local_src,
    created_at_utc                       as created_at_utc_src,
    cast(vat_percentage as number(38,6)) as vat_pct_src_386
  from {{ ref('stg_orders') }}
),

-- ---------- Canonical customer_id from stg_transactions ----------
tx as (
  select
    cast(order_id as varchar)    as order_id,
    cast(customer_id as varchar) as customer_id
  from {{ ref('stg_transactions') }}
),

-- ---------- Join (ids are VARCHAR on both sides) ----------
joined as (
  select
    a.*,
    o.vat_pct_src_386,
    t.customer_id
  from agg a
  left join orders o using (order_id)
  left join tx     t using (order_id)
),

-- ---------- Base for address matching ----------
orders_base as (
  select order_id, customer_id, created_at_utc
  from joined
),

-- ---------- Address book: USER_ID -> CUSTOMER_ID via stg_users ----------
-- NOTE: Build address_ts here from your columns (UPDATED_AT / _SRC_EXTRACTED_AT)
addr_user as (
  select
    u.customer_id,
    lower(coalesce(sa.address_type,'shipping')) as address_type,
    sa.area,
    sa.town,
    sa.region_id,
    sa.country_code,
    sa.address_display,
    sa.building,
    sa.apartment_number,
    sa.street_name,
    sa.landmark,
    sa.email,
    sa.phone,

    /* ✅ If columns are already numeric, just CAST them to the precision you want */
    cast(sa.latitude  as number(38,6)) as latitude,
    cast(sa.longitude as number(38,6)) as longitude,

    /* address timestamp: prefer UPDATED_AT, else fallback to _SRC_EXTRACTED_AT */
    coalesce(
      /* If these are already timestamps, TO_TIMESTAMP_NTZ is fine; if they’re strings, it will parse. */
      to_timestamp_ntz(sa.updated_at),
      to_timestamp_ntz(sa._src_extracted_at)
    ) as address_ts
  from {{ ref('stg_addresses') }} sa
  join {{ ref('stg_users') }}     u
    on cast(sa.user_id as varchar) = cast(u.user_id as varchar)
),


-- ---------- Best Shipping address at order time ----------
addr_at_order as (
  select
    ob.order_id,
    au.area,
    au.town,
    au.region_id,
    au.country_code,
    au.address_display,
    au.building,
    au.apartment_number,
    au.street_name,
    au.landmark,
    au.email,
    au.phone,
    au.latitude,
    au.longitude,
    row_number() over (
      partition by ob.order_id
      order by
        case when au.address_ts is not null and au.address_ts <= ob.created_at_utc then 0 else 1 end,
        abs(datediff('second', coalesce(au.address_ts, ob.created_at_utc), ob.created_at_utc))
    ) as rn
  from orders_base ob
  left join addr_user au
    on au.customer_id = ob.customer_id
   and au.address_type = 'shipping'
),

-- ---------- Final amounts & derived percentages ----------
finalized as (
  select
    j.*,
    cast(
      case
        when j.order_gross_386 is null or j.order_gross_386 = 0::number(38,6) then 0::number(38,6)
        else round( (j.order_discount_386 / j.order_gross_386) * 100::number(38,6), 2)
      end
    as number(38,6)) as order_discount_pct_386,
    cast(
      case
        when j.order_net_bt_386 is null or j.order_net_bt_386 = 0::number(38,6) then 0::number(38,6)
        else round( (j.order_tax_386 / j.order_net_bt_386) * 100::number(38,6), 2)
      end
    as number(38,6)) as order_vat_pct_eff_386
  from joined j
)

select
  md5(cast(coalesce(cast(f.order_id as text), '_dbt_utils_surrogate_key_null_') as text)) as order_sk,

  -- keys
  f.order_id,
  f.customer_id,

  -- timestamps
  f.created_at_local,
  f.created_at_utc,
  to_date(f.created_at_utc)   as order_date,
  to_date(f.created_at_local) as order_date_local,

  -- counts
  f.item_count,
  f.distinct_products,

  -- amounts (rounded; still 38,6)
  round(f.order_gross_386,    2) as order_gross,
  round(f.order_discount_386, 2) as order_discount_amt,
  round(f.order_net_bt_386,   2) as order_net_before_tax,
  round(f.order_tax_386,      2) as order_tax_amt,
  round(f.order_net_386,      2) as order_net_amt,

  -- percentages (0..100)
  round(f.order_discount_pct_386, 2) as order_discount_pct_effective,
  round(f.order_vat_pct_eff_386,  2) as order_vat_pct_effective,

  -- source VAT %
  round(f.vat_pct_src_386, 2) as vat_pct_src,

  -- 🚚 Shipping address (from stg_addresses schema)
  ao.area             as ship_area,
  ao.town             as ship_town,
  ao.region_id        as ship_region_id,
  ao.country_code     as ship_country_code,
  ao.address_display  as ship_address_display,
  ao.building         as ship_building,
  ao.apartment_number as ship_apartment_number,
  ao.street_name      as ship_street_name,
  ao.landmark         as ship_landmark,
  ao.email            as ship_email,
  ao.phone            as ship_phone,
  ao.latitude         as ship_latitude,
  ao.longitude        as ship_longitude,

  current_timestamp() as _calculated_at
from finalized f
left join addr_at_order ao
  on ao.order_id = f.order_id
 and ao.rn = 1
{% if is_incremental() %}
where f.created_at_utc >= dateadd(day, -7, current_timestamp())
{% endif %}