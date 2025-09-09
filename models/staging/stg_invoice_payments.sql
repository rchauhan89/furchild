{{ config(
    materialized='incremental',
    unique_key='payment_id',
    on_schema_change='append_new_columns'
) }}

with src as (
    select *
    from {{ source('bronze','zbooks_invoices') }}
),

j as (
    select
        _AIRBYTE_RAW_ID               as raw_id,
        _AIRBYTE_EXTRACTED_AT         as extracted_at,
        try_parse_json(JSON_DATA)     as j
    from src
),

base as (
    select
        j:invoice_id::string                 as invoice_id,
        j:invoice_number::string             as invoice_number,
        j:customer_id::string                as customer_id,
        j:customer_name::string              as customer_name,
        j:date::date                         as invoice_date,
        nullif(j:due_date::string,'')::date  as due_date,
        nullif(j:last_payment_date::string,'')::date as last_payment_date,
        upper(j:currency_code::string)       as currency_code,
        j:total::number(18,2)                as total_amount,
        coalesce(j:balance::number(18,2), 0) as balance,
        nullif(j:reference_number::string,'') as reference_number,
        lower(j:status::string)              as src_status,
        extracted_at                         as _src_extracted_at
    from j
),

norm as (
    select
        *,
        case 
          when src_status = 'paid' and balance = 0 then 'paid'
          when src_status in ('paid','partially_paid') and balance > 0 then 'partially_paid'
          when src_status = 'draft' then 'draft'
          when src_status = 'void'  then 'void'
          when src_status in ('unpaid','overdue') then 'unpaid'
          else coalesce(src_status,'unknown')
        end as status_canonical,
        greatest(coalesce(total_amount,0) - coalesce(balance,0), 0) as amount_paid,
        coalesce(
          last_payment_date,
          case when (coalesce(total_amount,0) - coalesce(balance,0)) > 0 then invoice_date end
        ) as payment_date
    from base
)

select
    {{ dbt_utils.generate_surrogate_key([
      "'zoho_invoice'",
      "invoice_id",
      "coalesce(to_char(payment_date), to_char(invoice_date))"
    ]) }}                            as payment_id,
    invoice_id,
    invoice_number,
    customer_id,
    customer_name,
    invoice_date,
    due_date,
    payment_date,
    status_canonical,
    total_amount,
    amount_paid,
    balance,
    currency_code,
    reference_number,
    _src_extracted_at
from norm
where status_canonical in ('paid','partially_paid')
