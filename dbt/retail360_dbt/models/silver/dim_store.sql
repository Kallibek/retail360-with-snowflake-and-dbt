{{ config(
    materialized='table',
    unique_key='store_id',
    on_schema_change='append_new_columns'
) }}

WITH latest_staging AS (
    SELECT
        store_id,
        store_name,
        store_type,
        city,
        state_province,
        country,
        postal_code,
        ingestion_ts,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY created_at DESC
        ) AS row_num
    FROM {{ ref('stg_stores') }}
    WHERE store_id IS NOT NULL
)

SELECT
    store_id,
    store_name,
    store_type,
    city,
    state_province,
    country,
    postal_code,
    created_at
FROM latest_staging
WHERE row_num = 1
