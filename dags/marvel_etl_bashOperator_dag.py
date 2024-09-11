from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'Luciano Alessi',
    'start_date': datetime(2024, 8, 27),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False, 
}

with DAG(
    dag_id='marvel_etl_pipeline_bashOperator',
    default_args=default_args,
    description='This DAG extracts data from the Marvel API, transforming and loading information about characters and comics. The pipeline performs a daily incremental extraction, saves the data in parquet format, and stores it in a data lake.',
    schedule_interval="@daily",  
    catchup=False
) as dag:
    
    test_api_connection_task = BashOperator(
        task_id='test_api_connection',
        bash_command='python /opt/airflow/scripts/api_test_connection.py'
    )

    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='python /opt/airflow/scripts/extract.py'
    )

    transform_task = BashOperator(
        task_id='transform_data',
        bash_command='python /opt/airflow/scripts/transform.py'
    )

    load_task = BashOperator(
        task_id='load_data_redshift',
        bash_command='python /opt/airflow/scripts/load.py'
    )


    test_api_connection_task >> extract_task >> transform_task >> load_task


