import os
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
import boto3

# Reuse your existing helper
from utils.snowflake_util import run_snowflake_query

default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def show_pipes_and_get_arn(**context):
    """
    Executes SHOW PIPES in Snowflake, finds the pipe(s) we care about,
    and returns the 'notification_channel' ARN(s).
    """
    results = run_snowflake_query("SHOW PIPES;")

    # "SHOW PIPES" returns rows with columns:
    # created_on, name, database_name, schema_name, definition, owner,
    # notification_channel, comment, options, pattern
    # The exact indexing may vary; let's map them by index carefully:

    # If you are not sure which pipe you want,
    # we can collect all pipes with their ARNs into a dict:
    pipe_arns = {}
    for row in results:
        # Typically row[1] = pipe_name, row[2] = database_name, row[3] = schema_name,
        # and row[6] = notification_channel. But this can vary by Snowflake version.
        pipe_name = row[1]         # name
        db_name = row[2]           # database_name
        schema_name = row[3]       # schema_name
        notification_channel = row[6]  # The ARN we need

        # You can filter if you only want a specific pipe:
        # if pipe_name.lower() not in ("orders_pipe", "orderitems_pipe"):
        #     continue

        logging.info(f"Found pipe: {db_name}.{schema_name}.{pipe_name} -> ARN: {notification_channel}")
        if notification_channel:
            pipe_arns[f"{db_name}.{schema_name}.{pipe_name}"] = notification_channel

    # XCom can only store 1 object. We'll store a dict { pipe_full_name: arn, ... }
    # The next task can parse what it needs.
    return pipe_arns


def configure_s3_bucket_notifications(**context):
    """
    Reads the pipe ARNs from XCom, then sets up S3 event notifications
    to point to each ARN (one SQS queue can handle all pipes for that bucket).
    """
    # XCom from previous task
    pipe_arns = context["ti"].xcom_pull(key="return_value", task_ids="show_pipes_task")
    if not pipe_arns:
        logging.warning("No pipes or ARNs found, skipping S3 notification configuration.")
        return

    # Typically, Snowflake sets up *one* SQS ARN per bucket (shared among multiple pipes).
    # So you might see the same ARN repeated for multiple pipes.
    # We'll just pick the first unique ARN we find:
    unique_arns = list(set(pipe_arns.values()))
    if not unique_arns:
        logging.warning("No valid notification_channel ARNs found, skipping.")
        return

    # We'll assume the first ARN is what we want:
    selected_arn = unique_arns[0]
    logging.info(f"Selected SQS ARN for notification: {selected_arn}")

    # S3 bucket (must match the bucket used by your Snowflake stage).
    bucket_name = os.environ.get("AWS_LANDING_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")

    # Connect to S3 via boto3
    # (Airflow's 'aws_default' or environment variables)
    session = boto3.Session()
    s3_client = session.client("s3")

    # Build the config for S3 event notifications:
    # You can add prefix filters if you only want certain subfolders (e.g. "orders/").
    # Also note: If your bucket already has a notification config, you might want to
    # merge rather than overwrite. The example below overwrites.

    notification_conf = {
        "QueueConfigurations": [
            {
                "Id": "SnowflakeAutoIngestOrders",
                "QueueArn": selected_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": "orders/"
                            }
                        ]
                    }
                }
            },
            {
                "Id": "SnowflakeAutoIngestOrderItems",
                "QueueArn": selected_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": "orderitems/"
                            }
                        ]
                    }
                }
            }
        ]
    }

    # Put the notification on the bucket
    logging.info(f"Setting S3 notification on bucket [{bucket_name}] -> SQS ARN: {selected_arn}")
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=notification_conf
    )
    logging.info("S3 Bucket Notification configured successfully.")

def create_snowpipe_for_orders_and_orderitems(**kwargs):
    """
    Creates two Snowpipes with auto-ingestion enabled:
     1) Orders pipe (auto-loads files from s3://.../orders/)
     2) Orderitems pipe (auto-loads files from s3://.../orderitems/)
    Assumes STAGING.LANDING_STAGE exists and references the S3 bucket.
    """
    from utils.snowflake_util import run_snowflake_query

    # You can run multiple queries in a list:
    queries = [
        # --- Create or replace the Orders pipe ---
        """
        CREATE OR REPLACE PIPE STAGING.orders_pipe
          AUTO_INGEST = TRUE
        AS
          COPY INTO STAGING.orders
          FROM @STAGING.LANDING_STAGE/orders/
          FILE_FORMAT = (FORMAT_NAME = STAGING.MY_CSV);
        """,

        # --- Create or replace the Orderitems pipe ---
        """
        CREATE OR REPLACE PIPE STAGING.orderitems_pipe
          AUTO_INGEST = TRUE
        AS
          COPY INTO STAGING.orderitems
          FROM @STAGING.LANDING_STAGE/orderitems/
          FILE_FORMAT = (FORMAT_NAME = STAGING.MY_CSV);
        """
    ]

    # Execute all queries in Snowflake
    run_snowflake_query(queries)

with DAG(
    dag_id="configure_snowpipe_notifications",
    default_args=default_args,
    description="Retrieves Snowpipe SQS ARN from SHOW PIPES; sets up S3 Notification via boto3",
    schedule_interval="@once",
    catchup=False,
) as dag:
    
    create_snowpipe_task = PythonOperator(
        task_id='create_snowpipe_for_orders_and_orderitems',
        python_callable=create_snowpipe_for_orders_and_orderitems,
    )

    show_pipes_task = PythonOperator(
        task_id="show_pipes_task",
        python_callable=show_pipes_and_get_arn,
    )

    configure_s3_notification_task = PythonOperator(
        task_id="configure_s3_notification_task",
        python_callable=configure_s3_bucket_notifications,
    )

    create_snowpipe_task >> show_pipes_task >> configure_s3_notification_task
