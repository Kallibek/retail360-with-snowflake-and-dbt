from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import logging
import os
import random
import pandas as pd
from io import StringIO

# Default arguments for the continuous orders data DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



def generate_and_upload_orders_data(**context):
    """
    Download the static customers, products, and stores data from S3,
    then generate synthetic orders and order items with a randomized row count
    between 400 and 1000. Upload the generated CSVs to S3 with a timestamp.
    """
    from utils.generate_data import generate_orders_and_orderitems

    s3_hook = S3Hook()
    bucket_name = os.environ.get('AWS_LANDING_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")

    # Download the static data CSVs from S3
    csv_customers = s3_hook.read_key(key="customers/latest.csv", bucket_name=bucket_name)
    csv_products = s3_hook.read_key(key="products/latest.csv", bucket_name=bucket_name)
    csv_stores = s3_hook.read_key(key="stores/latest.csv", bucket_name=bucket_name)

    # Convert CSV strings to DataFrames
    df_customers = pd.read_csv(StringIO(csv_customers))
    df_products = pd.read_csv(StringIO(csv_products))
    df_stores = pd.read_csv(StringIO(csv_stores))

    # Randomize the number of rows for orders (between 400 and 1000)
    row_count = random.randint(400, 1000)
    logging.info("Generating orders and order items data with row count: %s", row_count)

    # Generate orders and order items. (Assumes your utility function accepts a count parameter.)
    df_orders, df_orderitems = generate_orders_and_orderitems(df_customers, df_products, df_stores, num_orders=row_count)

    # Convert each DataFrame to CSV string (in-memory)
    csv_orders = df_orders.to_csv(index=False)
    csv_orderitems = df_orderitems.to_csv(index=False)

    # Create a timestamp to be included in the S3 file names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logging.info("Uploading generated orders CSV files to S3 bucket: %s", bucket_name)
    s3_hook.load_string(csv_orders, key=f"orders/orders_{timestamp}.csv", bucket_name=bucket_name, replace=True)
    s3_hook.load_string(csv_orderitems, key=f"orderitems/orderitems_{timestamp}.csv", bucket_name=bucket_name, replace=True)
    logging.info("Orders and order items data upload complete.")

with DAG(
    dag_id='generate_orders_data_continuous',
    default_args=default_args,
    description='Continuously generate synthetic orders and order items data every minute',
    schedule_interval='* * * * *',  # runs every minute
    catchup=False,
) as dag:
    
    orders_data_task = PythonOperator(
        task_id='generate_and_upload_orders_data',
        python_callable=generate_and_upload_orders_data,
    )

    orders_data_task
