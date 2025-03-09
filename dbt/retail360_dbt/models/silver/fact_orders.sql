{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    cluster_by=['created_at', 'order_id']
) }}

WITH latest_staging AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        order_status,
        /* Exclude or fix invalid totals. Here we just set them to 0 if negative. */
        CASE WHEN total_amount < 0 OR total_amount IS NULL THEN 0.0 ELSE total_amount END AS total_amount,
        /* discount_amount >= 0, if not, set to 0. Similar logic for shipping_cost. */
        CASE WHEN discount_amount < 0 OR discount_amount IS NULL THEN 0.0 ELSE discount_amount END AS discount_amount,
        CASE WHEN shipping_cost < 0 OR shipping_cost IS NULL THEN 0.0 ELSE shipping_cost END AS shipping_cost,
        payment_type,
        ingestion_ts,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY created_at DESC
        ) AS row_num
    FROM {{ ref('stg_orders') }}
    WHERE order_id IS NOT NULL
)

SELECT
    order_id,
    customer_id,
    store_id,
    order_status,
    total_amount,
    discount_amount,
    shipping_cost,
    payment_type,
    created_at
FROM latest_staging
WHERE row_num = 1
{% if is_incremental() %}
  AND created_at > COALESCE((SELECT max(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}