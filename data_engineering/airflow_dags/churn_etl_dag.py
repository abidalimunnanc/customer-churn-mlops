from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data-eng',
    'start_date': datetime(2025, 5, 13),
    'retries': 1,
}
 
with DAG('churn_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    ingest = BashOperator(
        task_id='ingest_data',
        bash_command='python3 /path/to/customer-churn-mlops/data-engineering/ingest_data.py'
    )

    clean = BashOperator(
        task_id='clean_data',
        bash_command='python3 /workspaces/customer-churn-mlops/data_engineering/clean_data_spark'
    )

    ingest >> clean
