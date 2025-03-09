{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

with orderitems as (
    select
        $1::TEXT as order_id,
        $2::TEXT as product_id,
        $3::NUMBER(6, 0) as quantity,
        $4::NUMBER(6, 2) as unit_price,
        $5::NUMBER(6, 2) as line_total,
        $6::NUMBER(6, 2) as line_discount,
        $7::timestamp_ntz as created_at,
        current_timestamp() as ingestion_ts
    FROM @LANDING_STAGE/orderitems/
        (FILE_FORMAT => 'MY_CSV')
)

select * from orderitems
{% if is_incremental() %}
  WHERE created_at > COALESCE((SELECT max(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}
