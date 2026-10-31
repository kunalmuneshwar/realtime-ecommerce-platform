
{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,

    COUNT(*) AS total_events,
    COUNT(DISTINCT customer_id) AS unique_customers,

    COUNT(*) FILTER (
        WHERE event_type = 'product_viewed'
    ) AS product_views,

    COUNT(*) FILTER (
        WHERE event_type = 'add_to_cart'
    ) AS add_to_cart_events,

    COUNT(*) FILTER (
        WHERE event_type = 'order_placed'
    ) AS order_events,

    COUNT(DISTINCT order_id) FILTER (
        WHERE event_type = 'order_placed'
    ) AS unique_orders,

    COUNT(*) FILTER (
        WHERE event_type = 'payment_success'
    ) AS successful_payments,

    COUNT(*) FILTER (
        WHERE event_type = 'payment_failed'
    ) AS failed_payments

FROM {{ ref('stg_events') }}

GROUP BY
    product_id,
    product_name
