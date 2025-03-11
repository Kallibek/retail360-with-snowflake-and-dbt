from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your dbt project directory path (adjust if needed)
DBT_PROJECT_DIR = '/opt/dbt/retail360_dbt'

with DAG(
    dag_id='dbt_docs_server_8081',
    default_args=default_args,
    description='Generate and serve dbt docs on port 8081',
    schedule_interval='@once',
    catchup=False,
) as dag:

    generate_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir ."
    )

    serve_docs = BashOperator(
        task_id='dbt_docs_serve',
        bash_command=f"cd {DBT_PROJECT_DIR} && nohup dbt docs serve --port 8081 --host 0.0.0.0 --profiles-dir . > /dev/null 2>&1 &"
    )

    generate_docs >> serve_docs
