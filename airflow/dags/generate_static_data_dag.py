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