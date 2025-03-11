import os
import logging
import json
import boto3
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import time

# Reuse your existing helper
from utils.snowflake_util import run_snowflake_query

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Constants ---
# AWS/IAM and Snowflake Integration
ROLE_NAME = "SnowflakeS3Role"         # IAM role name
POLICY_NAME = "SnowflakeS3Policy"       # IAM policy name
INTEGRATION_NAME = "S3_INT"             # Snowflake storage integration name

# Snowflake Schema and Stage Configuration
SCHEMA_NAME = os.environ.get('STAGING_SCHEMA')
STAGE_NAME = "LANDING_STAGE_P"
STAGE_FULL_NAME = f"{SCHEMA_NAME}.{STAGE_NAME}"
FILE_FORMAT_NAME = os.environ.get('CSV_FILE_FORMAT_NAME')
FILE_FORMAT_FULL_NAME = f"{SCHEMA_NAME}.{FILE_FORMAT_NAME}"

# Snowpipe and Pipe Configuration
PIPE_ORDERS_NAME = "ORDERS_PIPE"
PIPE_ORDERITEMS_NAME = "ORDERITEMS_PIPE"
TABLE_ORDERS = "orders"
TABLE_ORDERITEMS = "orderitems"
PREFIX_ORDERS = "orders/"
PREFIX_ORDERITEMS = "orderitems/"

# Dummy values for the IAM trust policy
DUMMY_PRINCIPAL = "*"
DUMMY_EXTERNAL_ID = "dummy-external-id"

# --- Task Functions ---
def create_iam_role_with_dummy_policy(**context):
    """
    Creates an IAM role with a dummy trust policy and attaches a read-only S3 policy.
    """
    iam_client = boto3.client("iam")
    bucket_name = os.environ.get("AWS_LANDING_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")
    
    # 1) Create role with dummy trust policy
    assume_role_policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": DUMMY_PRINCIPAL},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": DUMMY_EXTERNAL_ID}}
            }
        ]
    }
    logging.info("Creating IAM role with dummy trust policy...")
    try:
        role_resp = iam_client.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_doc)
        )
        role_arn = role_resp["Role"]["Arn"]
        logging.info(f"Created IAM role {ROLE_NAME}: {role_arn}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        logging.info(f"Role {ROLE_NAME} already exists. Retrieving its ARN...")
        existing_role = iam_client.get_role(RoleName=ROLE_NAME)
        role_arn = existing_role["Role"]["Arn"]

    # 2) Create a read-only S3 policy for our bucket
    logging.info("Creating S3 read-only policy for Snowflake...")
    s3_policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GetObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:GetBucketLocation",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            }
        ]
    }
    try:
        policy_resp = iam_client.create_policy(
            PolicyName=POLICY_NAME,
            PolicyDocument=json.dumps(s3_policy_doc)
        )
        policy_arn = policy_resp["Policy"]["Arn"]
        logging.info(f"Created IAM policy {POLICY_NAME}: {policy_arn}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        logging.info(f"Policy {POLICY_NAME} already exists. Retrieving its ARN...")
        sts_client = boto3.client("sts")
        account_id = sts_client.get_caller_identity()["Account"]
        existing_policy = iam_client.get_policy(
            PolicyArn=f"arn:aws:iam::{account_id}:policy/{POLICY_NAME}"
        )
        policy_arn = existing_policy["Policy"]["Arn"]

    # 3) Attach the policy to the role
    logging.info(f"Attaching policy {POLICY_NAME} to role {ROLE_NAME}...")
    iam_client.attach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn=policy_arn
    )

    # Push role ARN to XCom for downstream tasks
    return role_arn


def create_snowflake_storage_integration(**context):
    """
    Creates the Snowflake storage integration using the IAM role ARN.
    """
    role_arn = context["ti"].xcom_pull(task_ids="create_iam_role_with_dummy_policy")
    if not role_arn:
        raise ValueError("No role ARN found in XCom.")
    
    bucket_name = os.environ.get("AWS_LANDING_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")

    query = f"""
    CREATE OR REPLACE STORAGE INTEGRATION {INTEGRATION_NAME}
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '{role_arn}'
    STORAGE_ALLOWED_LOCATIONS = ('s3://{bucket_name}/');
    """
    logging.info(f"Creating Snowflake storage integration {INTEGRATION_NAME} ...")
    run_snowflake_query(query)


def describe_snowflake_integration_and_update_iam(**context):
    """
    Describes the Snowflake integration to retrieve the actual principal and external ID,
    then updates the IAM role trust policy.
    """
    desc_query = f"DESC INTEGRATION {INTEGRATION_NAME}"
    results = run_snowflake_query(desc_query)

    snowflake_principal_arn = None
    snowflake_external_id = None

    # Updated indices based on expected DESCRIBE output format:
    # Expected row format: (integration_name, property, property_value, ...)
    for row in results:
        prop = row[0]
        val = row[2]
        logging.info(f"Found integration property: {prop} = {val}")
        if prop == "STORAGE_AWS_IAM_USER_ARN":
            snowflake_principal_arn = val
        elif prop in ("STORAGE_AWS_EXTERNAL_ID", "EXTERNAL_ID"):
            snowflake_external_id = val

    logging.info(f"Snowflake principal ARN: {snowflake_principal_arn}")
    logging.info(f"Snowflake external ID: {snowflake_external_id}")

    if not snowflake_principal_arn or not snowflake_external_id:
        raise ValueError("Could not retrieve STORAGE_AWS_IAM_USER_ARN or EXTERNAL_ID from integration description.")

    iam_client = boto3.client("iam")
    current_policy_doc = iam_client.get_role(RoleName=ROLE_NAME)["Role"]["AssumeRolePolicyDocument"]

    # Update the trust policy with the actual Snowflake principal ARN.
    # Ensure the principal is in the correct format (a string or list of strings)
    current_policy_doc["Statement"][0]["Principal"]["AWS"] = snowflake_principal_arn
    current_policy_doc["Statement"][0]["Condition"]["StringEquals"]["sts:ExternalId"] = snowflake_external_id

    iam_client.update_assume_role_policy(
        RoleName=ROLE_NAME,
        PolicyDocument=json.dumps(current_policy_doc)
    )
    logging.info(f"Updated IAM role trust policy for {ROLE_NAME} with Snowflake principal and external ID.")


def create_snowflake_stage(**context):
    """
    Creates a Snowflake stage that references the S3 bucket and the storage integration.
    """
    bucket_name = os.environ.get("AWS_LANDING_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")
    
    query = f"""
    CREATE OR REPLACE STAGE {STAGE_FULL_NAME}
    URL = 's3://{bucket_name}/'
    STORAGE_INTEGRATION = {INTEGRATION_NAME}
    FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT_FULL_NAME});
    """
    logging.info(f"Creating Snowflake stage {STAGE_FULL_NAME} ...")
    run_snowflake_query(query)

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


def create_snowpipe_for_orders_and_orderitems(**kwargs):
    """
    Creates Snowpipes for orders and orderitems with auto-ingestion enabled.
    """

    # need 10 sec sleep. Otherwise it fails
    logging.info("Sleeping...")
    time.sleep(10)

    queries = [
        f"""
        CREATE OR REPLACE PIPE {SCHEMA_NAME}.{PIPE_ORDERS_NAME}
            AUTO_INGEST = TRUE
        AS
        COPY INTO {SCHEMA_NAME}.{TABLE_ORDERS} (
            order_id,
            customer_id,
            store_id,
            order_status,
            total_amount,
            discount_amount,
            shipping_cost,
            payment_type,
            created_at,
            ingestion_ts
        )
        FROM (
            SELECT 
                t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9,
                CURRENT_TIMESTAMP() AS ingestion_ts
            FROM @{STAGE_FULL_NAME}/{PREFIX_ORDERS} t
        )
        FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT_FULL_NAME});
        """,
        f"""
        CREATE OR REPLACE PIPE {SCHEMA_NAME}.{PIPE_ORDERITEMS_NAME}
        AUTO_INGEST = TRUE
        AS
        COPY INTO {SCHEMA_NAME}.{TABLE_ORDERITEMS} (
            order_id,
            product_id,
            quantity,
            unit_price,
            line_total,
            line_discount,
            created_at,
            ingestion_ts
        )
        FROM (
            SELECT 
                t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7,
                CURRENT_TIMESTAMP() AS ingestion_ts
            FROM @{STAGE_FULL_NAME}/{PREFIX_ORDERITEMS} t
        )
        FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT_FULL_NAME});
        """
    ]
    run_snowflake_query(queries)


def show_pipes_and_get_arn(**context):
    """
    Executes SHOW PIPES in Snowflake and returns a mapping of pipe full names to their notification ARNs.
    """
    results = run_snowflake_query("SHOW PIPES;")
    pipe_arns = {}
    for row in results:
        pipe_name = row[1]
        db_name = row[2]
        schema_name = row[3]
        notification_channel = row[6]
        logging.info(f"Found pipe: {db_name}.{schema_name}.{pipe_name} -> ARN: {notification_channel}")
        if notification_channel:
            pipe_arns[f"{db_name}.{schema_name}.{pipe_name}"] = notification_channel
    return pipe_arns


def configure_s3_bucket_notifications(**context):
    """
    Configures S3 event notifications to point to the Snowflake pipe notification ARN.
    """
    pipe_arns = context["ti"].xcom_pull(key="return_value", task_ids="show_pipes_task")
    if not pipe_arns:
        logging.warning("No pipes or ARNs found, skipping S3 notification configuration.")
        return

    unique_arns = list(set(pipe_arns.values()))
    if not unique_arns:
        logging.warning("No valid notification_channel ARNs found, skipping.")
        return

    selected_arn = unique_arns[0]
    logging.info(f"Selected SQS ARN for notification: {selected_arn}")

    bucket_name = os.environ.get("AWS_LANDING_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("AWS_LANDING_BUCKET_NAME environment variable is not set.")

    s3_client = boto3.Session().client("s3")
    notification_conf = {
        "QueueConfigurations": [
            {
                "Id": "SnowflakeAutoIngestOrders",
                "QueueArn": selected_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [{"Name": "prefix", "Value": PREFIX_ORDERS}]
                    }
                }
            },
            {
                "Id": "SnowflakeAutoIngestOrderItems",
                "QueueArn": selected_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [{"Name": "prefix", "Value": PREFIX_ORDERITEMS}]
                    }
                }
            }
        ]
    }
    logging.info(f"Setting S3 notification on bucket [{bucket_name}] -> SQS ARN: {selected_arn}")
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=notification_conf
    )
    logging.info("S3 Bucket Notification configured successfully.")


# --- Define the DAG and Tasks ---
with DAG(
    dag_id="configure_snowpipe",
    default_args=default_args,
    description="Creates and configures SnowPipe",
    schedule_interval="@once",
    catchup=False,
) as dag:

    create_iam_role_task = PythonOperator(
        task_id="create_iam_role_with_dummy_policy",
        python_callable=create_iam_role_with_dummy_policy,
    )

    create_integration_task = PythonOperator(
        task_id="create_snowflake_storage_integration",
        python_callable=create_snowflake_storage_integration,
    )

    finalize_iam_policy_task = PythonOperator(
        task_id="describe_integration_and_update_iam",
        python_callable=describe_snowflake_integration_and_update_iam,
    )

    create_stage_task = PythonOperator(
        task_id="create_snowflake_stage",
        python_callable=create_snowflake_stage,
    )

    create_tables_task = PythonOperator(
        task_id='create_orders_and_orderitems_tables',
        python_callable=create_orders_and_orderitems_tables,
    )

    create_snowpipe_task = PythonOperator(
        task_id="create_snowpipe_for_orders_and_orderitems",
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

    # Define task dependencies
    (
        create_iam_role_task 
        >> create_integration_task 
        >> finalize_iam_policy_task 
        >> create_stage_task 
        >> create_tables_task 
        >> create_snowpipe_task 
        >> show_pipes_task 
        >> configure_s3_notification_task
     )
