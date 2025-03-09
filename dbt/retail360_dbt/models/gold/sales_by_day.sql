{{ config(
    materialized='table',
    schema='MARTS',
    on_schema_change='append_new_columns'
) }}

WITH daily AS (
    SELECT
        DATE_TRUNC('DAY', fo.created_at)          AS order_date,
        COUNT(DISTINCT fo.order_id)               AS total_orders,
        SUM(fo.total_amount)                      AS total_sales,
        SUM(fo.discount_amount)                   AS total_discounts,
        SUM(fo.shipping_cost)                     AS total_shipping
    FROM {{ ref('fact_orders') }} fo
    GROUP BY 1
)

SELECT
    order_date,
    total_orders,
    total_sales,
    total_discounts,
    total_shipping
FROM daily
