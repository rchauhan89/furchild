{{ config(severity='warn') }}
with d as (
  select category_id, count(*) c from {{ ref('stg_product_categories') }} group by 1 having count(*) > 1
)
select c.*
from {{ ref('stg_product_categories') }} c
join d using (category_id)
