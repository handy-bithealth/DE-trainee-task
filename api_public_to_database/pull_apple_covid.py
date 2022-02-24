from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 2, 17) # 1 feb 2022 GMT 7 
}

with DAG(
    'pull_apple_covid', 
    schedule_interval='0 13 * * *', 
    default_args=default_args, 
    catchup=False  
) as dag:
    t1 = BashOperator(
        task_id='project_apple',
        bash_command='cd ~/airflow/script_python && python3 project_apple.py'
    )

    t2 = BashOperator(
        task_id='project_covid',
        bash_command='cd ~/airflow/script_python && python3 project_covid.py'
    )

    t3 = BashOperator(
        task_id='read_api_covid',
        bash_command='cd ~/airflow/script_python && python3 read_api.py'
    )

    t4 = BashOperator(
        task_id='moving_daily_covid',
        bash_command='cd ~/airflow/script_python && python3 moving_daily_covid.py'
    )


    [t1, t2 >> t3 >> t4]
