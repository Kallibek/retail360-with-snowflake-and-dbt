{% snapshot stores_snapshot %}
  {{
    config(
      target_schema='CORE',         
      unique_key='store_id',        
      strategy='timestamp',         
      updated_at='created_at',      
      invalidate_hard_deletes=true  
    )
  }}

WITH deduped AS (
    SELECT
      store_id,
      store_name,
      store_type,
      city,
      state_province,
      country,
      postal_code,
      created_at,
      ingestion_ts,
      ROW_NUMBER() OVER (
        PARTITION BY store_id, created_at 
        ORDER BY ingestion_ts DESC
      ) AS row_num
    FROM {{ ref('stg_stores') }}
  )

  SELECT
      store_id,
      store_name,
      store_type,
      city,
      state_province,
      country,
      postal_code,
      created_at,
      ingestion_ts
  FROM deduped
  WHERE row_num = 1

{% endsnapshot %}
