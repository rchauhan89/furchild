{{ config(materialized='table', unique_key='shipment_id') }}

with src as (
  select
    _AIRBYTE_RAW_ID,
    _AIRBYTE_EXTRACTED_AT,
    try_parse_json(JSON_DATA) as j
  from {{ source('bronze','zbooks_shipments') }}
),

base as (
  select
    j:shipment_id::string        as shipment_id,
    j:package_id::string         as package_id,
    j:package_number::string     as package_number,
    j:status::string             as src_status,
    j:tracking_number::string    as tracking_number,
    nullif(j:shipping_date::string,'')::date  as shipped_at,
    nullif(j:delivery_date::string,'')::date  as delivered_at,
    j:delivery_method::string    as delivery_method,
    j:date::date                 as shipment_created_date,  -- Zoho "date" at creation
    j:customer_id::string        as customer_id,
    j:customer_name::string      as customer_name,
    j:salesorder_id::string      as salesorder_id,
    j:salesorder_number::string  as salesorder_number
  from src
),

norm as (
  select
    *,
    case lower(coalesce(src_status,'')) 
      when 'shipped'         then 'shipped'
      when 'in_transit'      then 'in_transit'
      when 'delivered'       then 'delivered'
      when 'failed'          then 'failed'
      when 'returned'        then 'returned'
      else 'shipped'  -- Zoho exports often use 'shipped' for dispatched; adjust if you see others
    end as status_canonical
  from base
),

dedup as (
  -- if duplicates ever show up, keep the latest ingested row
  select
    *,
    row_number() over (partition by shipment_id order by shipment_created_date desc, shipped_at desc, delivered_at desc) as rn
  from norm
)

select
  shipment_id,
  package_id,
  package_number,
  status_canonical,
  tracking_number,
  shipped_at,
  delivered_at,
  delivery_method,
  shipment_created_date,
  customer_id,
  customer_name,
  salesorder_id,
  salesorder_number
from dedup
where rn = 1
