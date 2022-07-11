# Reference https://www.youtube.com/watch?v=IumQX-mm20Y
# Never use XCom for large files etc Pandas Dataframe. Its limited only 48Kb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'oguz',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(age, ti):
    name = ti.xcom_pull(task_ids='get_name')

    first_name = ti.xcom_pull(task_ids='get_name_v2',
                              key='first_name')

    last_name = ti.xcom_pull(task_ids='get_name_v2',
                             key='last_name')

    if not name:
        raise Exception('No name value.')

    if not [first_name, last_name]:
        raise Exception('Error on multiple xcom')

    print(f'Hello world! My name is {name},'
          f'and I am {age} years old')

    print(f'Hello world! My name is {first_name} {last_name},'
          f'and I am {age} years old')


def get_name():
    return 'Jerry'


def get_name_v2(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')


with DAG(
        dag_id='dag_python_operator_xcom',
        default_args=default_args,
        description='Our first dag using python operator',
        start_date=datetime(2022, 7, 1),
        catchup=False
) as dag:
    # Pass parameters using op_kwargs
    task_greet = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age': 20}
    )
    task_get_name = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task_get_name_v2 = PythonOperator(
        task_id='get_name_v2',
        python_callable=get_name_v2
    )

    [task_get_name, task_get_name_v2] >> task_greet
