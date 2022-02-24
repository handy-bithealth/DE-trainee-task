from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 2, 17) # 1 feb 2022 GMT 7 
}

with DAG(
    'project_dag_script_apple', 
    schedule_interval='0 8 * * *', 
    default_args=default_args, 
    catchup=False  
) as dag:
    t1 = BashOperator(
        task_id='moving_data',
        bash_command='cd ~/airflow/script_python && python3 project_apple.py'
    )

    t1
