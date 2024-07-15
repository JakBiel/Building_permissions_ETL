import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from aggregates_python_helpers import (
    download_and_unpack_zip,
    email_callback,
    load_permissions_to_bq,
    superior_aggregates_creator,
    validate_permissions_data,
)
from airflow import DAG
from airflow.operators.python import PythonOperator

# Use the absolute path if you need to navigate from the current working directory
absolute_path = '/opt/airflow/data/validation_results.html'

# Other global arguments
url = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia_2022_up.zip'
local_zip_path = 'zip_data.zip'
extract_to_folder = 'unpacked_zip_data_files'
csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2022_up.csv'

default_args = {
    'owner': 'James',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=2),
}

global_params = {
    'dataset_id': "airflow_dataset",
    'project_id': 'airflow-lab-415614',
    'table_id_name': 'permissions_results2022',
    'aggregate_table_name': 'new_aggregate_table'
}

# DAG definition
with DAG(
        default_args=default_args,
        dag_id='aggregates_python',
        description='Downloading given building permissions, saving in a database and generating aggregates',
        start_date=datetime(2023, 3, 2),
        schedule_interval='0 0 1 * *',
        catchup=True,
        max_active_runs=1
) as dag:
    
    zip_data_downloader_task = PythonOperator(
        task_id='zip_data_downloader_task',
        python_callable=download_and_unpack_zip,
        provide_context=True,
        op_args=[url, local_zip_path, extract_to_folder],
        dag=dag
    )

    validation_task = PythonOperator(
        task_id='validation_task',
        python_callable=validate_permissions_data,
        provide_context=True,
        op_args=[csv_file_path, absolute_path],
        dag=dag
    )
    
    unzipped_data_uploader_task = PythonOperator(
        task_id='unzipped_data_uploader',
        python_callable=load_permissions_to_bq,
        provide_context=True,
        op_args=[csv_file_path],
        op_kwargs={
            'params': global_params,
        },
        dag=dag
    )

    aggregates_creation_task = PythonOperator(
        task_id='aggregates_creation_task',
        python_callable=superior_aggregates_creator,
        provide_context=True,
        op_kwargs={
            'params': global_params,
        },
        dag=dag,
        execution_timeout=timedelta(minutes=8)
    )

    # Define the mail task
    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=email_callback,
        provide_context=True,
        dag=dag,
    )

# Task dependencies
zip_data_downloader_task >> validation_task >> unzipped_data_uploader_task >> aggregates_creation_task >> send_email_task
