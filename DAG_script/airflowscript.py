from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 1, 31) # 1 feb 2022 GMT 7 
}

with DAG(
    'emr_main', 
    schedule_interval='0 18 * * *', 
    default_args=default_args, 
    catchup=False  
) as dag:
    t1 = BashOperator(
        task_id='epres',
        bash_command='cd {path} && python3 epres.py {{ds}}'
    )

    t2 = BashOperator(
        task_id='labandradadoption',
        bash_command='cd {path} && python3 labandradadoption.py {{ds}}'
    )

    t3 = BashOperator(
        task_id='pharmacycatchment',
        bash_command='cd {path} && python3 pharmacycatchment.py {{ds}}'
    )
    
    t4 = BashOperator(
        task_id='soapandepres',
        bash_command='cd {path} && python3 soapandepres.py {{ds}}'
    )

    t1 >> t2 >> t3 >> t4