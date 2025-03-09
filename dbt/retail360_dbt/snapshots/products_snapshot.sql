{% snapshot products_snapshot %}
  {{
    config(
      target_schema='CORE',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='created_at',
      invalidate_hard_deletes=true
    )
  }}

  WITH deduped_products AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    created_at,
    ingestion_ts,
    ROW_NUMBER() OVER (PARTITION BY product_id, created_at ORDER BY ingestion_ts DESC) AS row_num
  FROM {{ ref('stg_products') }}
)

SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  created_at,
  ingestion_ts
FROM deduped_products
WHERE row_num = 1

{% endsnapshot %}
