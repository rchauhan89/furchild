{{ config(
    schema='SILVER',
    materialized='table',
    alias='STG_ORDER_ITEMS'
) }}

with src as (

    select
        "_AIRBYTE_EXTRACTED_AT"                           as _ingested_at,

        -- keys
        cast("ID" as string)                              as order_item_id,
        cast("TRANSACTION_ID" as string)                  as order_id,
        cast("PRODUCT_ID" as string)                      as product_id,
        cast("STOCK_ID" as string)                        as stock_id,

        -- product / inventory
        cast("PRODUCT_NAME" as string)                    as product_name,
        cast("UNIT" as string)                            as unit,
        cast("STOCK_NAME" as string)                      as stock_name,

        -- amounts
        cast("PRICE" as number(18,2))                     as price,
        cast("QUANTITY" as number(18,2))                  as quantity,
        cast("HAS_VAT" as boolean)                        as has_vat,

        -- discounts (wider precision to avoid overflow)
        cast("DISCOUNT_PERCENTAGE" as number(10,2))            as discount_percentage,
        cast("BULK_DISCOUNT_PERCENTAGE" as number(10,2))       as bulk_discount_percentage,
        cast("DISCOUNT_APPLICATION_TYPE" as int)               as discount_application_type,
        cast("FRIENDBUY_DISCOUNT_PERCENTAGE" as number(10,2))  as friendbuy_discount_percentage,
        cast("FRIENDBUY_DISCOUNT_APPLICATION_TYPE" as int)     as friendbuy_discount_application_type,

        -- extra fields
        cast("MEAL_PLAN_REQUEST_ID" as string)            as meal_plan_request_id

    from {{ source('bronze','transaction_items') }}

),

dedup as (
    select *,
           row_number() over (
             partition by order_item_id
             order by _ingested_at desc
           ) as _rn
    from src
)

select
    order_item_id,
    order_id,
    product_id,
    product_name,
    stock_id,
    stock_name,
    unit,

    quantity,
    price,
    (quantity * price) as line_amount,

    has_vat,
    discount_percentage,
    bulk_discount_percentage,
    discount_application_type,
    friendbuy_discount_percentage,
    friendbuy_discount_application_type,
    meal_plan_request_id,

    _ingested_at
from dedup
where _rn = 1
  and order_id is not null
