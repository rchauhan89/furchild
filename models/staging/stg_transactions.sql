{{ config(materialized='view') }}

-- NOTE: No is_paid / cancel / refund flags; just pass statuses through

with
-- =============================
-- Base Transactions (from Bronze)
-- =============================
tx as (
    select
        cast(id as varchar)                       as raw_order_id,
        to_date(transaction_date)                 as order_date,
        cast(user_id as varchar)                  as user_id_raw,   -- original app user ID
        lower(coalesce(device_type, 'unknown'))   as channel,
        trim(upper(coalesce(payment_status, '')))  as payment_status,
        trim(upper(coalesce(delivery_status, ''))) as delivery_status
    from {{ source('bronze', 'transactions') }}
),

-- =============================
-- Users (from stg_users)
-- =============================
u as (
  select
    cast(id as varchar)                  as user_id,
    cast(customer_account_id as varchar) as customer_id
  from {{ source('bronze','users') }}    -- direct source, bypasses ref('stg_users')
),


-- =============================
-- Joined & Enriched Transactions
-- =============================
src as (
    select
        tx.raw_order_id,
        tx.order_date,
        u.customer_id,                     -- ✅ canonical customer ID
        tx.channel,
        tx.payment_status,
        tx.delivery_status,
        tx.user_id_raw                     -- optional (for audit/debug)
    from tx
    left join u
        on tx.user_id_raw = u.user_id
)

-- =============================
-- Final Select
-- =============================
select
    /* Normalize order_id for consistent downstream joins */
    cast(
        regexp_replace(
            regexp_replace(
                regexp_replace(trim(raw_order_id),
                    '^(SO\\-|SO|ORD\\-|ORD|ORDER\\-|ORDER|#)', ''),   -- drop common prefixes
                '[-_ ]', ''),                                         -- remove -, _, spaces
            '[^[:alnum:]]', ''                                       -- keep only letters/digits
        ) as varchar
    ) as order_id,
    order_date,
    customer_id,
    channel,
    payment_status,
    delivery_status
from src
