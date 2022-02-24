from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 2, 24) # 24 feb 2022 GMT 7 
}

with DAG(
    'pull_weather_data', 
    schedule_interval='0 0 * * *', # 7 AM
    default_args=default_args, 
    catchup=False  
) as dag:
    t1 = BashOperator(
        task_id='pull_cirebon_data',
        bash_command='cd ~/airflow/script_python/weather && python3 pull_api_key.py {{ds}}'
    )



    t1
