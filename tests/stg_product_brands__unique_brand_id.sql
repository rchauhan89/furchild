{{ config(severity='warn') }}
with d as (
  select brand_id, count(*) c from {{ ref('stg_product_brands') }} group by 1 having count(*) > 1
)
select b.*
from {{ ref('stg_product_brands') }} b
join d using (brand_id)
