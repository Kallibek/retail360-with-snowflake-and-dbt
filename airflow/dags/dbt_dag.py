from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
with DAG(
    dag_id='dbt',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
) as dag:

    DBT_PROJECT_DIR = '/opt/dbt/retail360_dbt'  # adapt to your actual path

    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir ."
    )

    task_dbt_snapshot = BashOperator(
    task_id='dbt_snapshot',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir ."
    )


    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir ."
    )

    task_dbt_run >> task_dbt_snapshot >> task_dbt_test
