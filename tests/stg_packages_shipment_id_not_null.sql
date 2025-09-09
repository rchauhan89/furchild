{{ config(severity='warn') }}
select *
from {{ ref('stg_packages') }}
where shipment_id is null