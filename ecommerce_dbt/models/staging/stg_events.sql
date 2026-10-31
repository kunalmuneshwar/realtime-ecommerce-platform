{{ config(materialized='view') }}

SELECT
    event_id,
    LOWER(TRIM(event_type)) AS event_type,
    customer_id,
    order_id,
    product_id,
    NULLIF(TRIM(product_name), '') AS product_name,
    product_price,
    event_timestamp,
    ingested_at,
    event_timestamp::DATE AS event_date

FROM {{ source('ecommerce', 'events') }}