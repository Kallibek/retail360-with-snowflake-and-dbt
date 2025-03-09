{{ config(
    materialized='table',
    unique_key='product_id',
    on_schema_change='append_new_columns'
) }}

WITH latest_staging AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        ingestion_ts,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY created_at DESC
        ) AS row_num
    FROM {{ ref('stg_products') }}
    WHERE product_id IS NOT NULL
        AND price is not null
        AND price > 0
)

SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    created_at
FROM latest_staging
WHERE row_num = 1
