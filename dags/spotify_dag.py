from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': ['playtowin19@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description="dag for spotify ETL",
    schedule_interval=timedelta(days=1)
)


