{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

with orders as (
    select
         $1::string as order_id,
         $2::string as customer_id,
         $3::string as store_id,
         $4::string as order_status,
         $5::number(6,2) as total_amount,
         $6::number(6,2) as discount_amount,
         $7::number(6,2) as shipping_cost,
         $8::string as payment_type,
         $9::timestamp_ntz as created_at,
         current_timestamp() as ingestion_ts
    FROM @LANDING_STAGE/orders/
        (FILE_FORMAT => 'MY_CSV')
)

select * from orders
{% if is_incremental() %}
  WHERE created_at > COALESCE((SELECT max(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}
