from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import logging
import os

# Default arguments for the static data DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_and_upload_static_data(**context):
    """
    Generate synthetic data for customers, products, and stores,
    then upload them to S3 under fixed keys (latest) for future reference.
    """
    from utils.generate_data import generate_customers, generate_products, generate_stores

    logging.info("Generating synthetic customer, product, and store data")
    df_customers = generate_customers()
    df_products = generate_products()
    df_stores = generate_stores()

    # Convert each DataFrame to a CSV string (in-memory)
    csv_customers = df_customers.to_csv(index=False)
    csv_products = df_products.to_csv(index=False)
    csv_stores = df_stores.to_csv(index=False)

    # Instantiate S3Hook (AWS credentials provided via environment variables)
    s3_hook = S3Hook()
    bucket_name = os.environ.get('AWS_LANDING_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")

    logging.info("Uploading static CSV files to S3 bucket: %s", bucket_name)
    s3_hook.load_string(csv_customers, key="customers/latest.csv", bucket_name=bucket_name, replace=True)
    s3_hook.load_string(csv_products, key="products/latest.csv", bucket_name=bucket_name, replace=True)
    s3_hook.load_string(csv_stores, key="stores/latest.csv", bucket_name=bucket_name, replace=True)
    logging.info("Static data upload complete.")

def create_orders_and_orderitems_tables(**kwargs):
    """
    Create orders and orderitems tables in Snowflake manually.
    Adjust the schema (here: STAGING) or data types as needed.
    """
    from utils.snowflake_util import run_snowflake_query

    create_orders_table_query = """
    CREATE TABLE IF NOT EXISTS STAGING.ORDERS (
        order_id STRING,
        customer_id STRING,
        store_id STRING,
        order_status STRING,
        total_amount NUMBER(6,2),
        discount_amount NUMBER(6,2),
        shipping_cost NUMBER(6,2),
        payment_type STRING,
        created_at TIMESTAMP_NTZ,
        ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_orderitems_table_query = """
    CREATE TABLE IF NOT EXISTS STAGING.ORDERITEMS (
        order_id STRING,
        product_id STRING,
        quantity NUMBER(6,0),
        unit_price NUMBER(6,2),
        line_total NUMBER(6,2),
        line_discount NUMBER(6,2),
        created_at TIMESTAMP_NTZ,
        ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
    """

    logging.info("Creating ORDERS table in STAGING schema...")
    run_snowflake_query(create_orders_table_query)
    logging.info("ORDERS table created (if not exists).")

    logging.info("Creating ORDERITEMS table in STAGING schema...")
    run_snowflake_query(create_orderitems_table_query)
    logging.info("ORDERITEMS table created (if not exists).")

with DAG(
    dag_id='generate_static_data',
    default_args=default_args,
    description='Generate and upload static data (customers, products, and stores) to S3',
    schedule_interval='@once',
    catchup=False,
) as dag:

    static_data_task = PythonOperator(
        task_id='generate_and_upload_static_data',
        python_callable=generate_and_upload_static_data,
    )

    create_tables_task = PythonOperator(
        task_id='create_orders_and_orderitems_tables',
        python_callable=create_orders_and_orderitems_tables,
    )

    static_data_task >> create_tables_task
