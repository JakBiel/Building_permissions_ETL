import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from aggregates_python_helpers import (
    absolute_path,
    download_and_unpack_zip,
    email_callback,
    load_permissions_to_bq,
    superior_aggregates_creator,
    validate_permissions_data,
)
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from great_expectations.exceptions import GreatExpectationsError

default_args = {
    'owner': 'YOUR_NAME',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=2),
}

def main_of_zip_data_downloader(params):
    """Main function for ZIP data downloader."""
    url = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia_2022_up.zip'
    local_zip_path = 'zip_data.zip'
    extract_to_folder = 'unpacked_zip_data_files'
    download_and_unpack_zip(url, local_zip_path, extract_to_folder)

def main_of_validation(params):
    csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2022_up.csv'

    validate_permissions_data(csv_file_path, absolute_path)

def main_of_unzipped_data_uploader(params, **kwargs):
    # Path to the CSV file containing data to be validated
    csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2022_up.csv'

    load_permissions_to_bq(csv_file_path, params, **kwargs)

    logging.info("Data upload and validation completed.")


def main_of_aggregates_creation(params, **kwargs):
    """Main function for aggregates creation."""

    superior_aggregates_creator(params, **kwargs)

# DAG definition
with DAG(
        default_args=default_args,
        dag_id='aggregates_python',
        description='Downloading given building permissions, saving in a database and generating aggregates',
        start_date=datetime(2023, 3, 2),
        schedule_interval='0 0 1 * *',
        catchup=True,
        max_active_runs=1,
        params={
            'dataset_id': "airflow_dataset",
            'project_id': 'airflow-lab-415614',
            'table_id_name': 'permissions_results2022',
        }
        
) as dag:
    
    zip_data_downloader_task = PythonOperator(
        task_id='zip_data_downloader_task',
        python_callable=main_of_zip_data_downloader,
        provide_context=True,
        dag=dag
    )

    validation_task = PythonOperator(
        task_id='validation_task',
        python_callable=main_of_validation,
        provide_context=True,
        dag=dag
    )
    
    unzipped_data_uploader_task = PythonOperator(
        task_id='unzipped_data_uploader',
        python_callable=main_of_unzipped_data_uploader,
        provide_context=True,
        dag=dag
    )

    aggregates_creation_task = PythonOperator(
        task_id='aggregates_creation_task',
        python_callable=main_of_aggregates_creation,
        provide_context=True,
        dag=dag
    )

    # Define the mail task
    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=email_callback,
        provide_context=True,
        dag=dag,
    )

#Task dependencies
zip_data_downloader_task >> validation_task >> unzipped_data_uploader_task >> aggregates_creation_task >> send_email_task