{{ config(materialized='table', unique_key='address_sk') }}

with src as (
  select
    -- raw ids
    ID::string                                   as raw_id,
    USER_ADDRESS_ID::string                      as user_address_id,
    TRANSACTION_ID::string                       as transaction_id,
    USER_ID::string                              as user_id,

    -- name / contact
    nullif(trim(FIRST_NAME), '')                 as first_name,
    nullif(trim(LAST_NAME),  '')                 as last_name,
    lower(nullif(trim(EMAIL), ''))               as email,
    nullif(trim(PHONE), '')                      as phone,

    -- address lines
    nullif(trim(BUILDING), '' )                  as building,
    nullif(trim(APARTMENT_NUMBER), '')           as apartment_number,
    nullif(trim(STREET_NAME), '')                as street_name,
    nullif(trim(LAND_MARK), '')                  as landmark,
    nullif(trim(AREA), '')                       as area,
    nullif(trim(TOWN), '')                       as town,
    nullif(trim(POST_CODE), '')                  as post_code,
    nullif(trim(REGION_ID), '')                  as region_id,
    upper(nullif(trim(COUNTRY_CODE), ''))        as country_code,

    -- misc
    nullif(trim(COMPANY_NAME), '')               as company_name,
    lower(nullif(trim(TYPE), ''))                as address_type,       -- e.g., shipping/billing
    iff(upper(nullif(trim(IS_NEW_ADDRESS), '')) in ('1','TRUE','Y'),'TRUE','FALSE')::boolean as is_new_address,
    iff(upper(nullif(trim(IS_NEW_ADDRESS_TAG), '')) in ('1','TRUE','Y'),'TRUE','FALSE')::boolean as is_new_address_tag,

    -- geo (robust casts: varchar -> trim -> null -> try_to_decimal)
    try_to_decimal(nullif(trim(to_varchar(LATITUDE)),  ''), 9, 6)      as latitude,
    try_to_decimal(nullif(trim(to_varchar(LONGITUDE)), ''), 9, 6)      as longitude,

    -- timestamps (keep as timestamp if parseable, else null)
    try_to_timestamp_ntz(nullif(trim(to_varchar(DATE_UPDATED)), ''))    as updated_at,

    -- raw
    _AIRBYTE_EXTRACTED_AT                                             as _src_extracted_at
  from {{ source('bronze','transaction_addresses') }}
),

shaped as (
  select
    -- stable address id for dedupe: prefer user_address_id, else raw id
    coalesce(user_address_id, raw_id)            as address_id,
    user_address_id,
    raw_id,
    transaction_id,
    user_id,

    first_name,
    last_name,
    concat_ws(' ', first_name, last_name)        as full_name,

    company_name,
    address_type,
    email,
    phone,

    building,
    apartment_number,
    street_name,
    landmark,
    area,
    town,
    post_code,
    region_id,
    country_code,

    latitude,
    longitude,

    is_new_address,
    is_new_address_tag,

    updated_at,
    _src_extracted_at,

    -- a simple display line for convenience
    concat_ws(', ',
      nullif(concat_ws(' ', building, apartment_number, street_name), ''),
      nullif(concat_ws(' ', area, town), ''),
      nullif(concat_ws(' ', post_code, country_code), '')
    ) as address_display
  from src
),

dedup as (
  select
    *,
    row_number() over (
      partition by address_id
      order by coalesce(updated_at, _src_extracted_at) desc, raw_id desc
    ) as rn
  from shaped
)

select
  {{ dbt_utils.generate_surrogate_key(["address_id"]) }} as address_sk,
  address_id,
  user_address_id,
  raw_id,
  transaction_id,
  user_id,

  first_name,
  last_name,
  full_name,

  company_name,
  address_type,
  email,
  phone,

  building,
  apartment_number,
  street_name,
  landmark,
  area,
  town,
  post_code,
  region_id,
  country_code,

  -- clamp to valid ranges (optional but safe)
  iff(latitude  between -90  and 90,  latitude,  null) as latitude,
  iff(longitude between -180 and 180, longitude, null) as longitude,

  is_new_address,
  is_new_address_tag,

  updated_at,
  _src_extracted_at,
  address_display
from dedup
where rn = 1
