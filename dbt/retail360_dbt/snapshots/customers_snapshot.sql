{% snapshot customers_snapshot %}
  {{ 
    config(
      target_schema='CORE',
      unique_key='customer_id',      
      strategy='timestamp',
      updated_at='created_at',
      invalidate_hard_deletes=true
    ) 
  }}

WITH deduped AS (
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
        created_at,
        ingestion_ts,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, created_at 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM {{ ref('stg_customers') }}
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
    created_at,
    ingestion_ts
FROM deduped
WHERE row_num = 1

{% endsnapshot %}
