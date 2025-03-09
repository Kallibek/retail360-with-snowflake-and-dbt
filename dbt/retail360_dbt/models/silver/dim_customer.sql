{{ config(materialized='table', 
          unique_key='customer_id', 
          on_schema_change='append_new_columns') }}

WITH latest_staging AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        phone_number,
        address,
        city,
        state_province,
        country,
        postal_code,
        ingestion_ts,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY created_at DESC
        ) as row_num
    FROM {{ ref('stg_customers') }}
    WHERE first_name IS NOT NULL  -- simple cleansing rule
      AND last_name IS NOT NULL
      AND customer_id IS NOT NULL
)

SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    city,
    state_province,
    country,
    postal_code,
    created_at
FROM latest_staging
WHERE row_num = 1  -- pick the latest record for each customer
