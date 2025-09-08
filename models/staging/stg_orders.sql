{{ config(
    schema='SILVER',
    materialized='table',
    alias='STG_ORDERS'
) }}

with src as (

    select
        -- ingestion metadata
        "_AIRBYTE_EXTRACTED_AT"                         as _ingested_at,

        -- keys
        cast("ID" as string)                            as order_id,
        cast("USER_ID" as string)                       as customer_id,

        -- timestamps (Bronze already TIMESTAMP_NTZ)
        "TRANSACTION_DATE"                              as created_at_local,   -- keep local
        convert_timezone('Asia/Dubai','UTC',"TRANSACTION_DATE") as created_at_utc,
        convert_timezone('Asia/Dubai','UTC',"DATE_UPDATED")     as updated_at_utc,

        -- amounts (already numeric in Bronze → cast to a stable precision)
        cast("GROSS_AMOUNT" as number(18,2))            as gross_amount,
        cast("TOTAL_AMOUNT" as number(18,2))            as total_amount,
        cast("DISCOUNT_AMOUNT" as number(18,2))         as discount_amount,
        cast("BW_DISCOUNT_AMOUNT" as number(18,2))      as bw_discount_amount,
        cast("NDC_DISCOUNT_AMOUNT" as number(18,2))     as ndc_discount_amount,
        cast("BULK_DISCOUNT_AMOUNT" as number(18,2))    as bulk_discount_amount,
        cast("POINTS_DISCOUNT_AMOUNT" as number(18,2))  as points_discount_amount,
        cast("SPECIAL_DISCOUNT_AMOUNT" as number(18,2)) as special_discount_amount,
        cast("GIFT_CARD_AMOUNT_DISCOUNT" as number(18,2)) as giftcard_discount_amount,

        cast("VAT_PERCENTAGE" as number(5,2))           as vat_percentage,

        -- raw statuses
        "PAYMENT_METHOD"                                as payment_method_raw,
        "PAYMENT_STATUS"                                as payment_status_raw,
        "DELIVERY_STATUS"                               as delivery_status_raw,

        -- operational (force to string to avoid numeric/empty issues)
        cast("COUPON_CODE" as string)                   as coupon_code,
        cast("DEVICE_TYPE" as string)                   as device_type,
        cast("DELIVERY_OPTION" as string)               as delivery_option,
        cast("FRIENDBUY_CODE" as string)                as friendbuy_code,
        cast("GIFT_CARD_CODE" as string)                as gift_card_code,
        cast("ADDITIONAL_NOTE" as string)               as additional_note,
        cast("ZOHO_SO_ID" as string)                    as zoho_so_id,
        cast("FULL_NAME" as string)                     as customer_name

    from {{ source('bronze','transactions') }}

),

dedup as (
    select *,
           row_number() over (
             partition by order_id
             order by _ingested_at desc
           ) as _rn
    from src
),

clean as (
    select
        order_id,
        customer_id,

        created_at_local,
        created_at_utc,
        updated_at_utc,

        to_date(created_at_local) as order_date_local,
        to_date(created_at_utc)   as order_date_utc,

        gross_amount,
        total_amount,
        vat_percentage,

        coalesce(discount_amount,0)
          + coalesce(bw_discount_amount,0)
          + coalesce(ndc_discount_amount,0)
          + coalesce(bulk_discount_amount,0)
          + coalesce(points_discount_amount,0)
          + coalesce(special_discount_amount,0)
          + coalesce(giftcard_discount_amount,0)       as discount_total,

        case when payment_status_raw = 1 then 'paid'
             when payment_status_raw = 0 then 'unpaid'
             else 'unknown' end                        as payment_status,

        case when delivery_status_raw = 1 then 'delivered'
             when delivery_status_raw = 0 then 'pending'
             else 'unknown' end                        as delivery_status,

        payment_method_raw,
        coupon_code,
        device_type,
        delivery_option,
        friendbuy_code,
        gift_card_code,
        additional_note,
        zoho_so_id,
        customer_name,

        _ingested_at
    from dedup
    where _rn = 1
      and order_id is not null
)

select * from clean
