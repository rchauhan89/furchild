{{ 
  config(
    schema='GOLD',
    alias='DIM_CUSTOMERS',
    materialized='table',
    on_schema_change='sync_all_columns'
  ) 
}}

with src as (
  select
    cast(CUSTOMER_ID as varchar)   as customer_id,
    lower(trim(EMAIL))              as email,
    trim(PHONE)                     as phone_raw,
    trim(FULL_NAME)                 as full_name_raw,
    CREATED_AT,
    UPDATED_AT
  from {{ ref('stg_customers') }}
  where CUSTOMER_ID is not null
),

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
    email,

    -- Remove '+' signs from phone numbers
    case
      when phone_raw is null then null
      else regexp_replace(phone_raw, '\\+', '')
    end as phone,

    -- Remove everything AFTER 'Furchild:' (case-insensitive)
    -- Example:
    -- 'John Doe Furchild: VIP Client' → 'John Doe'
    -- 'Furchild: Cat Parent' → ''
    trim(
      regexp_replace(
        full_name_raw,
        '(?i)Furchild:.*$',   -- everything from 'Furchild:' to end of string
        ''
      )
    ) as full_name,

    created_at,
    updated_at
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
