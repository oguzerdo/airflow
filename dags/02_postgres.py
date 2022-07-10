import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Get postgress connection object
connection = PostgresHook.get_connection("postgres_db")
#get postgress URI
URI = connection.get_uri()


def get_iris_data():
    sql_query = 'SELECT * FROM iris'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='workshop'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_query)
    return cursor.fetchall()


def process_iris_data(ti):
    iris = ti.xcom_pull(task_ids=['get_iris_data'])
    if not iris:
        raise Exception('No data.')

    iris = pd.DataFrame(
        data=iris[0],
        columns=['irisid', 'sepallengthcm', 'sepalwidthcm',
                 'petallengthcm', 'petalwidthcm', 'species']
    )
    iris = iris[
        (iris['sepallengthcm'] > 5) &
        (iris['sepalwidthcm'] == 3) &
        (iris['petallengthcm'] > 3) &
        (iris['petalwidthcm'] == 1.5)
    ]
    iris = iris.drop('irisid', axis=1)
    iris.to_csv(Variable.get('tmp_iris_csv_location'), index=False)


with DAG(
        dag_id='postgres_db_dag',
        schedule_interval='@daily',
        start_date=datetime(year=2022, month=7, day=1),
        catchup=False
) as dag:
    # Task 1. Get the Iris data from a table in Postgres
    task_get_iris_data = PythonOperator(
        task_id='get_iris_data',
        python_callable=get_iris_data,
        do_xcom_push=True # push the returned value to Airflow's Xcoms
    )

    # Task 2. Process the Iris Data
    task_process_iris_data = PythonOperator(
        task_id='process_iris_data',
        python_callable=process_iris_data
    )

    # Task 3. Truncate Postgres table
    task_truncate_table = PostgresOperator(
        task_id='truncate_tgt_table',
        postgres_conn_id='postgres_db',
        sql='TRUNCATE TABLE iris_tgt'
    )

    # Task 4. Loading CSV file into Postgres with Airflow
    task_load_iris_data = BashOperator(
        task_id='load_iris_data',
        bash_command=(
            f'psql {URI} -c "'
            '\COPY iris_tgt(sepallengthcm, sepalwidthcm,petallengthcm, petalwidthcm, species) '
            'FROM "/tmp/iris_processed.csv" '
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )

    task_get_iris_data >> task_process_iris_data >> task_truncate_table >> task_load_iris_data