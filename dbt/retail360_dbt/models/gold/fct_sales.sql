{{ config(
    materialized='table',
    schema='MARTS',
    on_schema_change='append_new_columns',
    cluster_by=['order_created_at']
) }}

WITH order_items AS (
    SELECT
        foi.order_id,
        SUM(foi.line_total)       AS total_line_amount,
        SUM(foi.line_discount)    AS total_line_discount,
        COUNT(DISTINCT foi.product_id) AS distinct_products
    FROM {{ ref('fact_orderitems') }} foi
    GROUP BY foi.order_id
),

orders_enriched AS (
    SELECT
        fo.order_id,
        fo.order_status,
        fo.total_amount,
        fo.discount_amount,
        fo.shipping_cost,
        fo.payment_type,
        fo.created_at               AS order_created_at,
        fo.customer_id,
        fo.store_id,
        dc.first_name,
        dc.last_name,
        dc.country                  AS customer_country,
        ds.store_name,
        ds.country                  AS store_country,
        oi.total_line_amount,
        oi.total_line_discount,
        oi.distinct_products
    FROM {{ ref('fact_orders') }} fo
    LEFT JOIN order_items oi
        ON fo.order_id = oi.order_id
    LEFT JOIN {{ ref('dim_customer') }} dc
        ON fo.customer_id = dc.customer_id
    LEFT JOIN {{ ref('dim_store') }} ds
        ON fo.store_id = ds.store_id
)

SELECT
    order_id,
    order_status,
    total_amount,
    discount_amount,
    shipping_cost,
    payment_type,
    order_created_at,
    customer_id,
    first_name,
    last_name,
    customer_country,
    store_id,
    store_name,
    store_country,
    total_line_amount,
    total_line_discount,
    distinct_products
FROM orders_enriched
