{{ config(materialized='table', unique_key='product_detail_sk') }}

with src as (
  select
    /* Clean everything to trimmed TEXT; do zero numeric/date casts in staging */
    trim(to_varchar(PRODUCT_ID))        as product_id,
    nullif(trim(to_varchar(DETAIL_NAME)),   '') as detail_name,
    nullif(trim(to_varchar(DETAIL_TYPE)),   '') as detail_type,
    nullif(trim(to_varchar(DETAIL_CONTENT)),'') as detail_content,
    nullif(trim(to_varchar(SORT_ORDER)),    '') as sort_order,      -- keep as text
    lower(nullif(trim(to_varchar(STATUS)),  '')) as src_status,
    nullif(trim(to_varchar(DATE_CREATED)),  '') as created_at,      -- keep as text
    nullif(trim(to_varchar(LOCATION_INSERT)),'') as location_insert
  from {{ source('bronze','product_details') }}
),

base as (
  select
    {{ dbt_utils.generate_surrogate_key([
      "product_id",
      "coalesce(detail_name,'')",
      "coalesce(detail_type,'')",
      "coalesce(sort_order,'')"
    ]) }} as product_detail_sk,
    *
  from src
),

dedup as (
  select *,
         row_number() over (partition by product_detail_sk order by created_at desc nulls last) as rn
  from base
)

select
  product_detail_sk,
  product_id,
  detail_name,
  detail_type,
  detail_content,
  sort_order,         -- text
  src_status,
  created_at,         -- text
  location_insert
from dedup
where rn = 1
