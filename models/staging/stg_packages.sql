{{ config(materialized='table', unique_key='package_id') }}

with src as (
  select
    PACKAGE_ID::string           as package_id,
    SHIPMENT_ID::string          as shipment_id,
    CUSTOMER_ID::string          as customer_id,
    PACKAGE_NUMBER::string       as package_number,

    -- timestamps from Bronze
    DATE_CREATED::timestamp      as created_at,
    DELIVERY_DATE::date          as delivered_at,
    DELIVERY_TIME::string        as delivery_time,   -- keep raw; parse to time if you need

    -- operational attrs you had in Bronze packages
    ADMIN_ID::string             as admin_id,
    ORGANIZATION::string         as organization,
    BB_COUNT::number             as bb_count,
    CB_COUNT::number             as cb_count,
    BATCH_TIME::timestamp        as batch_time,
    IS_BB_VERIFIED::boolean      as is_bb_verified
  from {{ source('bronze','packages_printed') }}
),

dedup as (
  select
    *,
    row_number() over (
      partition by package_id
      order by coalesce(created_at, batch_time) desc, package_number desc
    ) as rn
  from src
)

select
  package_id,
  shipment_id,
  customer_id,
  package_number,
  created_at,
  delivered_at,
  delivery_time,
  admin_id,
  organization,
  bb_count,
  cb_count,
  batch_time,
  is_bb_verified
from dedup
where rn = 1
