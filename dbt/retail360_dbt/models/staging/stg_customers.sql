{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

WITH source_data AS (
    SELECT
        $1::string as customer_id,
        $2::string as first_name,
        $3::string as last_name,
        $4::string as email,
        $5::string as phone_number,
        $6::string as address,
        $7::string as city,
        $8::string as state_province,
        $9::string as country,
        $10::string as postal_code,
        $11::timestamp_ntz as created_at,
        current_timestamp() as ingestion_ts
    FROM @LANDING_STAGE/customers/
        (FILE_FORMAT => 'MY_CSV')
)

SELECT * FROM source_data

{% if is_incremental() %}
  WHERE created_at > COALESCE((SELECT max(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}
