{{ config(materialized='table', unique_key='brand_id') }}

with src as (
  select
    ID::string                                 as brand_id,
    nullif(NAME,'')                            as brand_name,
    nullif(SLUG,'')                            as brand_slug,
    try_to_timestamp_ntz(to_varchar(DATE_CREATED)) as created_at
  from {{ source('bronze','product_brands') }}
),
dedup as (
  select *,
         row_number() over (partition by brand_id order by created_at desc nulls last) as rn
  from src
)
select brand_id, brand_name, brand_slug, created_at
from dedup
where rn = 1
