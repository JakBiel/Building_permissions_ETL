from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta


default_args = {
    'owner': 'YOUR_NAME',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}


def hello_world():
    """
    Prints 'Hello World!' to the console.
    """
    print("Hello World!")

with DAG(
        default_args=default_args,
        dag_id='download_permissions',
        description='Downloading given building permissions',
        start_date=datetime(2023, 6, 1),
) as dag:

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world,
        dag=dag
    )

    hello_world