{{
  config(
    schema='GOLD',
    alias='DIM_CUSTOMERS',
    materialized='table',
    on_schema_change='sync_all_columns'
  )
}}

-- Source: SILVER.STG_CUSTOMERS
-- Columns used: CUSTOMER_ID, CUSTOMER_NAME, EMAIL_PRIMARY, EMAIL_OTHER,
--               PHONE_PRIMARY, PHONE_SECONDARY, PHONE_FORMATTED,
--               CREATED_AT, UPDATED_AT

with src as (
  select
    cast(CUSTOMER_ID as varchar)  as customer_id,
    trim(CUSTOMER_NAME)           as customer_name_raw,
    lower(trim(EMAIL_PRIMARY))    as email_primary,
    lower(trim(EMAIL_OTHER))      as email_other,
    trim(PHONE_PRIMARY)           as phone_primary,
    trim(PHONE_SECONDARY)         as phone_secondary,
    trim(PHONE_FORMATTED)         as phone_formatted,
    CREATED_AT,
    UPDATED_AT
  from {{ ref('stg_customers') }}
  where CUSTOMER_ID is not null
),

-- Prefer most recent per CUSTOMER_ID
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

    -- email: prefer primary, else other
    coalesce(email_primary, email_other) as email,

    -- phone: prefer primary -> secondary -> formatted; remove '+'
    case
      when coalesce(phone_primary, phone_secondary, phone_formatted) is null then null
      else regexp_replace(coalesce(phone_primary, phone_secondary, phone_formatted), '\\+', '')
    end as phone,

    -- name: remove everything AFTER 'Furchild:' (case-insensitive via 'i' param),
    -- then trim any trailing separators left behind.
    -- Step 1: drop "Furchild:.*$"
    -- Step 2: remove trailing '-', '–', ':' plus extra spaces
    trim(
      regexp_replace(
        regexp_replace(
          customer_name_raw,
          'Furchild:.*$',               -- pattern
          '',                           -- replacement
          1, 0, 'i'                     -- position, occurrence, parameters='i' (case-insensitive)
        ),
        '\\s*[-–:]+\\s*$',              -- trailing separators near end
        ''
      )
    ) as full_name,

    CREATED_AT as created_at,
    UPDATED_AT as updated_at
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
from cleaned
