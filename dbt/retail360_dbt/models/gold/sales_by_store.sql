{{ config(
    materialized='table',
    schema='MARTS',
    on_schema_change='append_new_columns'
) }}

WITH store_sales AS (
    SELECT
        fo.store_id,
        ds.store_name,
        ds.store_type,
        DATE_TRUNC('DAY', fo.created_at) AS order_date,
        COUNT(DISTINCT fo.order_id)      AS total_orders,
        SUM(fo.total_amount)             AS total_sales,
        SUM(fo.discount_amount)          AS total_discounts,
        SUM(fo.shipping_cost)            AS total_shipping
    FROM {{ ref('fact_orders') }} fo
    LEFT JOIN {{ ref('dim_store') }} ds
        ON fo.store_id = ds.store_id
    GROUP BY
        fo.store_id,
        ds.store_name,
        ds.store_type,
        DATE_TRUNC('DAY', fo.created_at)
)

SELECT *
FROM store_sales
