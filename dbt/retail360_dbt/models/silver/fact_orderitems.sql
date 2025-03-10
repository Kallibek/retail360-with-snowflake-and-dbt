{{ config(
    materialized='incremental',
    unique_key='order_product_key',
    on_schema_change='append_new_columns',
    cluster_by=['created_at', 'order_id','product_id']
) }}

WITH latest_staging AS (
    SELECT
        order_id,
        product_id,
        /* Handle negative or null quantities. Setting them to 0 or discarding is your choice. */
        CASE WHEN quantity < 0 OR quantity IS NULL THEN 0 ELSE quantity END AS quantity,
        CASE WHEN unit_price < 0 OR unit_price IS NULL THEN 0.0 ELSE unit_price END AS unit_price,
        CASE WHEN line_total < 0 OR line_total IS NULL THEN 0.0 ELSE line_total END AS line_total,
        /* Assume line_discount cannot be negative. If it can, set to 0 instead of negative. */
        CASE WHEN line_discount < 0 OR line_discount IS NULL THEN 0.0 ELSE line_discount END AS line_discount,
        ingestion_ts,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, product_id
            ORDER BY created_at DESC
        ) AS row_num
    FROM {{ source('staging', 'orderitems') }}
    WHERE order_id IS NOT NULL
      AND product_id IS NOT NULL
)

SELECT
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    line_discount,
    created_at,
    order_id || '_' || product_id AS order_product_key
FROM latest_staging
WHERE row_num = 1
{% if is_incremental() %}
  AND created_at > COALESCE((SELECT max(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}
