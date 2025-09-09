{{ config(materialized='table', unique_key='category_id') }}

with src as (
  select
    ID::string                                     as category_id,
    nullif(NAME,'')                                as category_name,
    nullif(SLUG,'')                                as category_slug,
    try_to_timestamp_ntz(to_varchar(DATE_CREATED)) as created_at
  from {{ source('bronze','product_categories') }}
),
dedup as (
  select *,
         row_number() over (partition by category_id order by created_at desc nulls last) as rn
  from src
)
select category_id, category_name, category_slug, created_at
from dedup
where rn = 1
