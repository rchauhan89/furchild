{{ config(materialized='table') }}

with j as (
    select try_parse_json(JSON_DATA) as j 
    from {{ source('bronze','zbooks_invoices') }}
)
select
    j:invoice_id::string       as invoice_id,
    j:invoice_number::string   as invoice_number,
    j:customer_id::string      as customer_id,
    j:customer_name::string    as customer_name,
    j:date::date               as invoice_date,
    nullif(j:due_date::string,'')::date as due_date,
    lower(j:status::string)    as src_status,
    case 
      when lower(j:status::string) = 'paid' and j:balance::number = 0 then 'paid'
      when j:balance::number > 0 then 'partially_paid'
      else 'unpaid'
    end as status_canonical,
    j:total::number(18,2)      as total_amount,
    coalesce(j:balance::number(18,2),0) as balance,
    greatest(coalesce(j:total::number,0) - coalesce(j:balance::number,0), 0) as amount_paid,
    upper(j:currency_code::string) as currency_code
from j
where coalesce(j:balance::number,0) > 0
