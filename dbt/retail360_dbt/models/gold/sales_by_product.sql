{{ config(
    materialized='table',
    schema='MARTS',
    on_schema_change='append_new_columns'
) }}

WITH product_sales AS (
    SELECT
        product_id,
        COUNT(*) AS total_items_sold,
        SUM(line_total) AS total_sales,
        SUM(line_discount) AS total_discounts
    FROM {{ ref('fact_orderitems') }}
    GROUP BY product_id
)

SELECT
    ps.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    dp.price,
    ps.total_items_sold,
    ps.total_sales,
    ps.total_discounts
FROM product_sales ps
LEFT JOIN {{ ref('dim_product') }} dp
    ON ps.product_id = dp.product_id
