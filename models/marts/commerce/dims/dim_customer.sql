{{
  config(
    schema='GOLD',
    alias='DIM_CUSTOMERS',
    materialized='table',
    on_schema_change='sync_all_columns'
  )
}}

-- Source: silver.stg_customers (exact columns seen in your sample)
-- Columns used: CUSTOMER_ID, CUSTOMER_NAME, EMAIL_PRIMARY, EMAIL_OTHER,
--               PHONE_PRIMARY, PHONE_SECONDARY, PHONE_FORMATTED,
--               CREATED_AT, UPDATED_AT

with src as (
  select
    cast(CUSTOMER_ID as varchar)            as customer_id,
    trim(CUSTOMER_NAME)                     as customer_name_raw,
    lower(trim(EMAIL_PRIMARY))              as email_primary,
    lower(trim(EMAIL_OTHER))                as email_other,
    trim(PHONE_PRIMARY)                     as phone_primary,
    trim(PHONE_SECONDARY)                   as phone_secondary,
    trim(PHONE_FORMATTED)                   as phone_formatted,
    CREATED_AT,
    UPDATED_AT
  from {{ ref('stg_customers') }}
  where CUSTOMER_ID is not null
),

-- Prefer most recent row per customer_id
ranked as (
  select
    *,
    row_number() over (
      partition by customer_id
      order by UPDATED_AT desc nulls last, CREATED_AT desc nulls last
    ) as rn
  from src
),

cleaned as (
  select
    customer_id,

    -- Email preference: primary, else other
    coalesce(email_primary, email_other)      as email,

    -- Phone preference: primary -> secondary -> formatted
    -- Remove '+' only (as requested), keep other characters as-is
    case
      when coalesce(phone_primary, phone_secondary, phone_formatted) is null then null
      else regexp_replace(coalesce(phone_primary, phone_secondary, phone_formatted), '\\+', '')
    end                                       as phone,

    -- Remove everything AFTER 'Furchild:' (case-insensitive).
    -- Then trim leftover separators (trailing '-' '–' or ':') and extra spaces.
    -- Examples:
    --   'Tristan ... - Furchild: Billy, Ruby' -> 'Tristan ... -'
    --   then post-trim of trailing separators -> 'Tristan ...'
    trim(
      regexp_replace(
        regexp_replace(customer_name_raw, '(?i)Furchild:.*$', ''),
        '\\s*[-–:]+\\s*$',  -- drop trailing separators/spaces left behind
        ''
      )
    )                                         as full_name,

    CREATED_AT                                as created_at,
    UPDATED_AT                                as updated_at
  from ranked
  where rn = 1
)

select
  {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
  customer_id,
  email,
  phone,
  full_name,
  created_at,
  updated_at
from cleaned;
