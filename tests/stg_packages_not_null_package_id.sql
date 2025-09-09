{{ config(severity='warn') }}
select *
from {{ ref('stg_packages') }}
where package_id is null
