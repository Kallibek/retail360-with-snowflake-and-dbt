{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

with stores as (
    select
         $1::string as store_id,
         $2::string as store_name,
         $3::string as store_type,
         $4::string as city,
         $5::string as state_province,
         $6::string as country,
         $7::string as postal_code,
         $8::timestamp_ntz as created_at,
         current_timestamp() as ingestion_ts
    FROM @LANDING_STAGE/stores/
        (FILE_FORMAT => 'MY_CSV')
)

select * from stores
{% if is_incremental() %}
  WHERE created_at > COALESCE((SELECT max(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}

