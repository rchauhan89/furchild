{{ config(
    schema='SILVER',
    materialized='table',
    alias='STG_CUSTOMERS'
) }}

with src as (

    select
        "_AIRBYTE_EXTRACTED_AT"                as _ingested_at,

        -- primary key
        cast("ID" as string)                   as customer_id,

        -- core profile
        cast("ACCOUNT_NAME" as string)         as customer_name,
        cast("ACCOUNT_EMAIL_ADDRESS" as string) as email_primary,
        cast("OTHER_EMAIL_ADDRESSES" as string) as email_other,

        -- phones
        cast("T_PHONE_NUMBER" as string)       as phone_primary,
        cast("T_SECONDARY_PHONE_NUMBER" as string) as phone_secondary,
        cast("FORMATTED_PHONES" as string)     as phone_formatted,

        -- address info
        cast("AREA" as string)                 as area,
        cast("BUILDING" as string)             as building,
        cast("APARTMENT_NUMBER" as string)     as apartment_number,
        cast("T_ADDRESS" as string)            as address_text,
        cast("ADDRESS_NAME" as string)         as address_name,
        cast("LAND_MARK" as string)            as landmark,
        cast("T_CITY_ID" as string)            as city_id,
        cast("T_COUNTRY" as string)            as country_id,

        -- geo
        try_cast("LATITUDE"  as float) as lat_raw,
        try_cast("LONGITUDE" as float) as lon_raw,
        -- flags
        cast("HAS_STAR" as boolean)            as has_star,
        cast("IS_FRAGILE" as boolean)          as is_fragile,
        cast("IS_INVALID" as boolean)          as is_invalid,
        cast("IS_ONLINE_ONLY" as boolean)      as is_online_only,

        -- pets & contact names
        cast("S_PET_NAMES" as string)          as pet_names,
        cast("S_PET_OWNER_NAMES" as string)    as pet_owner_names,
        cast("S_CONTACT_NAMES" as string)      as contact_names,

        -- system references
        cast("ZCRM_ACCOUNT_ID" as string)      as crm_account_id,
        cast("ZBOOKS_ACCOUNT_ID" as string)    as books_account_id,
        cast("ADMIN_ACCOUNT_ID" as string)     as admin_account_id,
        cast("PREV_ADMIN_ACCOUNT_ID" as string) as prev_admin_account_id,
        cast("ACCOUNT_REFERENCE" as string)    as account_reference,
        cast("DEFAULT_USER_ID" as string)      as default_user_id,
        cast("DEFAULT_USER_ADDRESS_ID" as string) as default_user_address_id,

        -- activity dates
        "DATE_CREATED"  as created_at,
        "DATE_UPDATED"  as updated_at,
        to_date("LAST_DELIVERY_DATE") as last_delivery_date,

        -- relocation
        cast("RELOCATED_COUNTRY_ID" as string) as relocated_country_id,

        -- notes & misc
        cast("DEFAULT_COMMENT" as string)      as default_comment,
        cast("T_ADDITIONAL_NOTE" as string)    as additional_note,
        cast("INVALID_COMMENTS" as string)     as invalid_comments,
        cast("MARI_COMMENT" as string)         as mari_comment,
        cast("UNPAID_SO_SMS" as boolean)       as unpaid_so_sms,
        cast("DELIVERY_SMS" as boolean)        as delivery_sms

    from {{ source('bronze','customer_accounts') }}

),

dedup as (
    select s.*
    from src s
    qualify row_number() over (
        partition by customer_id
        order by coalesce(updated_at, created_at) desc, _ingested_at desc
    ) = 1
)

select
  customer_id,
  customer_name,
  email_primary,
  email_other,
  phone_primary,
  phone_secondary,
  phone_formatted,
  area,
  building,
  apartment_number,
  address_text,
  address_name,
  landmark,
  city_id,
  country_id,

  /* validated geo: null if outside plausible bounds */
  cast(
    case when lat_raw between -90 and 90
         then round(lat_raw, 6)
         else null end
    as number(18,6)
  ) as latitude,

  cast(
    case when lon_raw between -180 and 180
         then round(lon_raw, 6)
         else null end
    as number(18,6)
  ) as longitude,

  has_star,
  is_fragile,
  is_invalid,
  is_online_only,
  pet_names,
  pet_owner_names,
  contact_names,
  crm_account_id,
  books_account_id,
  admin_account_id,
  prev_admin_account_id,
  account_reference,
  default_user_id,
  default_user_address_id,

  created_at,
  updated_at,
  last_delivery_date,
  relocated_country_id,
  default_comment,
  additional_note,
  invalid_comments,
  mari_comment,
  unpaid_so_sms,
  delivery_sms,
  _ingested_at
from dedup
where customer_id is not null

