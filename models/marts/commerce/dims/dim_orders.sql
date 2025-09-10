{{ config(
  materialized     = 'table',
  unique_key       = 'order_sk',
  on_schema_change = 'append_new_columns'
) }}

with src as (
  select
    cast(order_id    as varchar) as order_id,
    cast(customer_id as varchar) as customer_id,      -- NEW: customer linkage
    created_at_local,
    created_at_utc

    -- ────────────────────────────────────────────────────────────────
    -- Uncomment and map these as they become available in stg_orders:
    -- , status
    -- , payment_status
    -- , fulfillment_status
    -- , order_channel
    -- , currency_code
    -- , customer_email
    -- , shipping_country
    -- , shipping_city
    -- , shipping_postcode
    -- , coupon_code
    -- , notes
    -- ────────────────────────────────────────────────────────────────
  from {{ ref('stg_orders') }}
),

-- Rank orders per customer by creation time (stable tie-break on order_id)
ranked as (
  select
    s.*,
    row_number() over (
      partition by s.customer_id
      order by s.created_at_utc, s.order_id
    ) as order_rank_for_customer
  from src s
)

select
  -- Surrogate key aligned with facts
  md5(cast(coalesce(cast(order_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as order_sk,

  -- Business keys
  order_id,
  customer_id,

  -- Core timestamps
  created_at_local,
  created_at_utc,
  to_date(created_at_local) as order_date_local,

  -- Customer lifecycle
  case when order_rank_for_customer = 1 then true else false end as is_first_order,
  order_rank_for_customer

  -- ────────────────────────────────────────────────────────────────
  -- Pass-through additional attributes once uncommented in src:
  -- , status
  -- , payment_status
  -- , fulfillment_status
  -- , order_channel
  -- , currency_code
  -- , customer_email
  -- , shipping_country
  -- , shipping_city
  -- , shipping_postcode
  -- , coupon_code
  -- , notes
  -- ────────────────────────────────────────────────────────────────

from ranked
