# Taskflow API
# Reference: https://www.youtube.com/watch?v=9y0mqWsok_4
# Taskflow API get Xcoms automatically
# Also you don't need to set task dependencies

from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'oguz',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='dag_with_taskflow_api',
     default_args=default_args,
     start_date=datetime(2022, 7, 1),
     schedule_interval='@daily',
     catchup=False)
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Jery',
            'last_name': 'Fridman'
        }

    @task()
    def get_age():
        return 19

    @task()
    def greet(first_name, last_name, age):
        print(f'Hello my name is {first_name} {last_name} '
              f'and I am {age} years old.')

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          age=age)


taskflow_dag = hello_world_etl()
