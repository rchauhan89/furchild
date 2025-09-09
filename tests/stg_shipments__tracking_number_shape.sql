{{ config(severity='warn') }}

-- You can adjust the minimum length if you want stricter validation
select *
from {{ ref('stg_shipments') }}
where tracking_number is not null
  and length(tracking_number) < 1
