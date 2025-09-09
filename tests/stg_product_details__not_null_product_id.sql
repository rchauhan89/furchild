{{ config(severity='warn') }}
select *
from {{ ref('stg_product_details') }}
where product_id is null
