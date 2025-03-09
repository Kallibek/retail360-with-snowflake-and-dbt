{{ config(
    materialized='table',
    schema='MARTS',
    on_schema_change='append_new_columns'
) }}

WITH customer_orders AS (
    SELECT
        dc.customer_id,
        dc.first_name,
        dc.last_name,
        dc.country      AS customer_country,
        COUNT(DISTINCT fo.order_id) AS total_orders,
        SUM(fo.total_amount)        AS lifetime_spend,
        MIN(fo.created_at)          AS first_order_date,
        MAX(fo.created_at)          AS last_order_date
    FROM {{ ref('dim_customer') }} dc
    LEFT JOIN {{ ref('fact_orders') }} fo
        ON dc.customer_id = fo.customer_id
    GROUP BY
        dc.customer_id,
        dc.first_name,
        dc.last_name,
        dc.country
)

SELECT *
FROM customer_orders
