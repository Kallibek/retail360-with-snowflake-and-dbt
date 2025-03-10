from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import logging
import os
from utils.snowflake_util import run_snowflake_query



# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_and_upload_data(**context):
    """
    Generate synthetic data using functions in generate_data.py,
    convert the DataFrames to CSV strings, and upload them to an S3 bucket.
    """
    # Import synthetic data generators from generate_data.py
    from utils.generate_data import (
        generate_customers,
        generate_products,
        generate_stores,
        generate_orders_and_orderitems,
    )

    # Generate synthetic data
    logging.info("Generating synthetic customer, product, and store data")
    df_customers = generate_customers()
    df_products = generate_products()
    df_stores = generate_stores()
    df_orders, df_orderitems = generate_orders_and_orderitems(df_customers, df_products, df_stores)

    # Convert each DataFrame to CSV string (in-memory)
    csv_customers = df_customers.to_csv(index=False)
    csv_products = df_products.to_csv(index=False)
    csv_stores = df_stores.to_csv(index=False)
    csv_orders = df_orders.to_csv(index=False)
    csv_orderitems = df_orderitems.to_csv(index=False)

    # Create a timestamp to use in the S3 file names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Instantiate S3Hook (AWS credentials are available via env vars)
    s3_hook = S3Hook()

    # Get bucket name from environment variable
    bucket_name = os.environ.get('AWS_LANDING_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")

    # Upload CSV files to S3 under unique keys
    logging.info("Uploading generated CSV files to S3 bucket: %s", bucket_name)
    s3_hook.load_string(csv_customers, key=f"customers/customers_{timestamp}.csv", bucket_name=bucket_name, replace=True)
    s3_hook.load_string(csv_products, key=f"products/products_{timestamp}.csv", bucket_name=bucket_name, replace=True)
    s3_hook.load_string(csv_stores, key=f"stores/stores_{timestamp}.csv", bucket_name=bucket_name, replace=True)
    s3_hook.load_string(csv_orders, key=f"orders/orders_{timestamp}.csv", bucket_name=bucket_name, replace=True)
    s3_hook.load_string(csv_orderitems, key=f"orderitems/orderitems_{timestamp}.csv", bucket_name=bucket_name, replace=True)
    logging.info("Data upload complete.")




# Define the DAG
with DAG(
    dag_id='generate_synthetic_data',
    default_args=default_args,
    description='Generate synthetic data and upload it to the landing S3 bucket',
    schedule_interval='@once',
    catchup=False,
) as dag:

    generate_upload_task = PythonOperator(
        task_id='generate_and_upload_data',
        python_callable=generate_and_upload_data,
    )


    # run_query = PythonOperator(
    #     task_id='run_snowflake_query',
    #     python_callable=run_snowflake_query,
    #     op_kwargs={'query': "SELECT 1 + 1"}
    # )

    
    generate_upload_task
