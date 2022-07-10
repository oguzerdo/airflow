import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def process_datetime(ti):
    dt = ti.xcom_pull(task_ids=['get_datetime'])

    if not dt:
        raise Exception('No datetime value.')

    dt = str(dt[0]).split()

    return {
        'year': int(dt[-1]),
        'month': dt[1],
        'day': int(dt[2]),
        'time': dt[3],
        'day_of_week': dt[0]
    }

def save_datetime(ti):
    dt_processed = ti.xcom_pull(task_ids=['process_datetime'])

    if not dt_processed:
        raise Exception('No processed datetime value.')

    df = pd.DataFrame(dt_processed)

    csv_path = Variable.get('first_dag_csv_path')   # get path from variables
    if os.path.exists(csv_path):                    # check file is exists
        df_header = False
        df_mode = 'a'                               # if exists append mode
    else:
        df_header = True                            # don't write header another time
        df_mode = 'w'                               # if not exists write mode

    df.to_csv(csv_path, index=False, mode=df_mode, header=df_header)

with DAG(
        dag_id='first_airflow_dag',
        schedule_interval='* * * * * ',
        start_date=datetime(year=2022, month=7, day=1),
        catchup=False,
) as dag:
    # Task 1. Get current datetime
    task_get_datetime = BashOperator(
        task_id='get_datetime',
        bash_command='date'
    )
    # Task 2. Process current datetime
    task_process_datetime = PythonOperator(
        task_id="process_datetime",
        python_callable=process_datetime
    )

    # Task 3. Save processed datetime
    task_save_datetime = PythonOperator(
        task_id='save_datetime',
        python_callable=save_datetime
    )

    task_get_datetime >> task_process_datetime >> task_save_datetime

    # or
    # task_save_datetime << task_process_datetime << task_get_datetime
