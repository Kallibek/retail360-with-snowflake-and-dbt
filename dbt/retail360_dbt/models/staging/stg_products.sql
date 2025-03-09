{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

with products as (
    select
         $1::string as product_id,
         $2::string as product_name,
         $3::string as category,
         $4::string as brand,
         $5::number(6,2) as price,
         $6::timestamp_ntz as created_at,
         current_timestamp() as ingestion_ts
    FROM @LANDING_STAGE/products/
        (FILE_FORMAT => 'MY_CSV')
)

select * from products
{% if is_incremental() %}
  WHERE created_at > COALESCE((SELECT max(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}
