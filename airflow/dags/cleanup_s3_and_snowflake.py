from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import logging
import os
import shutil

# Reuse the same Snowflake helper from your environment
from utils.snowflake_util import run_snowflake_query

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def delete_all_s3_objects(**kwargs):
    """
    Deletes all objects (files) in the specified S3 bucket, but not the bucket itself.
    """
    bucket_name = os.environ.get('AWS_LANDING_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")
    
    logging.info(f"Deleting all objects from S3 bucket: {bucket_name}")
    s3_hook = S3Hook(aws_conn_id='aws_default')  # or your custom connection ID
    keys = s3_hook.list_keys(bucket_name=bucket_name)
    if keys:
        logging.info(f"Found {len(keys)} objects. Deleting...")
        s3_hook.delete_objects(bucket=bucket_name, keys=keys)
        logging.info("All objects deleted.")
    else:
        logging.info("No objects found in bucket.")

def drop_all_snowflake_tables(**kwargs):
    """
    Drops all tables in the STAGING, CORE, and MARTS schemas of the configured Snowflake database,
    without dropping the schemas themselves.
    """
    schemas = ["STAGING", "CORE", "MARTS"]
    for schema in schemas:
        logging.info(f"Dropping all tables in schema: {schema}")
        # 1) List all tables in the schema
        show_tables_query = f"SHOW TABLES IN {schema}"
        tables_result = run_snowflake_query(show_tables_query)

        # The `SHOW TABLES` query returns rows with table info; typically,
        # the first column is the table name. Adjust indexing as needed for your environment.
        table_names = [row[1] for row in tables_result]  # row[1] is often the TABLE_NAME

        # 2) Drop each table
        for table_name in table_names:
            drop_query = f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE"
            run_snowflake_query(drop_query)
            logging.info(f"Dropped table: {schema}.{table_name}")


def delete_dbt_directories(**kwargs):
    """
    Deletes all contents within the specified DBT directories:
    /opt/dbt/retail360_dbt/logs and /opt/dbt/retail360_dbt/target.
    """
    dirs_to_clean = ["/opt/dbt/retail360_dbt/logs", "/opt/dbt/retail360_dbt/target"]
    for directory in dirs_to_clean:
        if os.path.exists(directory):
            logging.info(f"Cleaning directory: {directory}")
            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)
                try:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)
                        logging.info(f"Deleted file: {item_path}")
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        logging.info(f"Deleted directory: {item_path}")
                except Exception as e:
                    logging.error(f"Failed to delete {item_path}. Reason: {e}")
            logging.info(f"Finished cleaning directory: {directory}")
        else:
            logging.warning(f"Directory {directory} does not exist.")

with DAG(
    dag_id='cleanup_s3_and_snowflake',
    default_args=default_args,
    description='Deletes all objects from S3 bucket and all tables from Snowflake (STAGING, CORE, MARTS).',
    schedule_interval='@once',
    catchup=False
) as dag:

    delete_s3_files = PythonOperator(
        task_id='delete_s3_files',
        python_callable=delete_all_s3_objects,
    )

    drop_snowflake_tables = PythonOperator(
        task_id='drop_snowflake_tables',
        python_callable=drop_all_snowflake_tables,
    )

    delete_dbt_files = PythonOperator(
        task_id='delete_dbt_files',
        python_callable=delete_dbt_directories,
    )

    delete_s3_files >> drop_snowflake_tables >> delete_dbt_files
