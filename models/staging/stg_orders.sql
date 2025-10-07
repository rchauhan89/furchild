{{ config(
    schema='SILVER',
    materialized='table',
    alias='STG_ORDERS'
) }}

with src as (
    select
        "_AIRBYTE_EXTRACTED_AT"                               as _ingested_at,

        -- keys
        cast("ID" as string)                                  as order_id_raw,
        cast("USER_ID" as string)                             as user_id_raw,   -- <-- was CUSTOMER_ID

        -- timestamps
        "TRANSACTION_DATE"                                    as created_at_local,
        coalesce(
          convert_timezone('Asia/Dubai','UTC',"TRANSACTION_DATE"),
          "_AIRBYTE_EXTRACTED_AT"
        )                                                     as created_at_utc,
        convert_timezone('Asia/Dubai','UTC',"DATE_UPDATED")   as updated_at_utc,

        -- delivery date + raw slot
        try_to_date("DELIVERY_DATE")                          as expected_delivery_date,
        lower(
          trim(
            regexp_replace(
              replace(replace(cast("DELIVERY_TIME" as string),'–','-'),'—','-'),
              '\\s+', ' '
            )
          )
        )                                                     as delivery_time_slot_norm,

        -- amounts
        cast("GROSS_AMOUNT" as number(18,2))                  as gross_amount,
        cast("TOTAL_AMOUNT" as number(18,2))                  as total_amount,
        cast("DISCOUNT_AMOUNT" as number(18,2))               as discount_amount,
        cast("BW_DISCOUNT_AMOUNT" as number(18,2))            as bw_discount_amount,
        cast("NDC_DISCOUNT_AMOUNT" as number(18,2))           as ndc_discount_amount,
        cast("BULK_DISCOUNT_AMOUNT" as number(18,2))          as bulk_discount_amount,
        cast("POINTS_DISCOUNT_AMOUNT" as number(18,2))        as points_discount_amount,
        cast("SPECIAL_DISCOUNT_AMOUNT" as number(18,2))       as special_discount_amount,
        cast("GIFT_CARD_AMOUNT_DISCOUNT" as number(18,2))     as giftcard_discount_amount,

        cast("VAT_PERCENTAGE" as number(5,2))                 as vat_percentage,

        -- raw flags
        "PAYMENT_METHOD"                                      as payment_method_raw,
        "PAYMENT_STATUS"                                      as payment_status_raw,
        "DELIVERY_STATUS"                                     as delivery_status_raw,

        -- operational (force to string)
        cast("COUPON_CODE" as string)                         as coupon_code,
        cast("DEVICE_TYPE" as string)                         as device_type,
        cast("DELIVERY_OPTION" as string)                     as delivery_option,
        cast("FRIENDBUY_CODE" as string)                      as friendbuy_code,
        cast("GIFT_CARD_CODE" as string)                      as gift_card_code,
        cast("ADDITIONAL_NOTE" as string)                     as additional_note,
        cast("ZOHO_SO_ID" as string)                          as zoho_so_id,
        cast("FULL_NAME" as string)                           as customer_name
    from {{ source('bronze','transactions') }}
),

-- normalize order_id exactly like stg_transactions
norm as (
    select
      *,
      cast(
        regexp_replace(
          regexp_replace(
            regexp_replace(trim(order_id_raw),
              '^(SO\\-|SO|ORD\\-|ORD|ORDER\\-|ORDER|#)', ''),
            '[-_ ]', ''),
          '[^[:alnum:]]',''
        ) as string
      ) as order_id
    from src
),

join_slot as (
    select
      s.*,
      d.delivery_slot_id,
      d.delivery_slot as delivery_time_slot
    from norm s
    left join {{ ref('stg_delivery_slots') }} d
      on s.delivery_time_slot_norm = d.slot_txt
),

dedup as (
    select s.*
    from join_slot s
    qualify row_number() over (
        partition by order_id
        order by coalesce(updated_at_utc, created_at_utc) desc, _ingested_at desc
    ) = 1
),

-- bring in canonical customer_id from stg_transactions
tx as (
  select order_id, customer_id
  from {{ ref('stg_transactions') }}
),

clean as (
    select
        s.order_id,
        t.customer_id,                 -- ✅ from stg_transactions

        s.created_at_local,
        s.created_at_utc,
        s.updated_at_utc,

        to_date(s.created_at_local) as order_date_local,
        to_date(s.created_at_utc)   as order_date_utc,

        -- keep requested delivery fields
        s.expected_delivery_date,
        s.delivery_slot_id,
        s.delivery_time_slot,

        s.gross_amount,
        s.total_amount,
        s.vat_percentage,

        coalesce(s.discount_amount,0)
        + coalesce(s.bw_discount_amount,0)
        + coalesce(s.ndc_discount_amount,0)
        + coalesce(s.bulk_discount_amount,0)
        + coalesce(s.points_discount_amount,0)
        + coalesce(s.special_discount_amount,0)
        + coalesce(s.giftcard_discount_amount,0) as discount_total,

        case when s.payment_status_raw = 1 then 'paid'
             when s.payment_status_raw = 0 then 'unpaid'
             else 'unknown' end                as payment_status,

        case when s.delivery_status_raw = 1 then 'delivered'
             when s.delivery_status_raw = 0 then 'pending'
             else 'unknown' end                as delivery_status,

        s.payment_method_raw,
        s.coupon_code,
        s.device_type,
        s.delivery_option,
        s.friendbuy_code,
        s.gift_card_code,
        s.additional_note,
        s.zoho_so_id,
        s.customer_name,

        s._ingested_at
    from dedup s
    left join tx t using (order_id)
    where s.order_id is not null
)

select * from clean
