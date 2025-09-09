{{ config(severity='warn') }}
select p.*
from {{ ref('stg_packages') }} p
left join {{ ref('stg_shipments') }} s
  on p.shipment_id = s.shipment_id
where s.shipment_id is null
