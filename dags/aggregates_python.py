import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from aggregates_python_helpers import (
    absolute_path,
    download_and_unpack_zip,
    html_content,
    load_data_from_csv_to_db,
    superior_aggregates_creator,
    validation,
)
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from great_expectations.exceptions import GreatExpectationsError
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

default_args = {
    'owner': 'YOUR_NAME',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=2),
}

def main_of_unzipped_data_uploader(ti):
    # Path to the CSV file containing data to be validated
    csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2022_up.csv'

    # Database connection parameters
    db_params = {
        'host': "zip_data_postgres_db",
        'dbname': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD')
    }

    # Initialize a database connection
    conn = psycopg2.connect(
        dbname=db_params['dbname'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host']
    )

    # Pass 'conn' as an argument to the 'load_data_from_csv_to_db' function
    load_data_from_csv_to_db(csv_file_path, db_params, ti)

    logging.info("Data upload and validation completed.")

def main_of_zip_data_downloader(ti):
    """Main function for ZIP data downloader."""
    url = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia_2022_up.zip'
    local_zip_path = 'zip_data.zip'
    extract_to_folder = 'unpacked_zip_data_files'
    download_and_unpack_zip(url, local_zip_path, extract_to_folder)

def main_of_validation(ti):
    csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2022_up.csv'

    validation(csv_file_path)

def main_of_aggregates_creation(ti):
    """Main function for aggregates creation."""
    # Set connection parameters

    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    postgres_db = os.environ.get('POSTGRES_DB')

    superior_aggregates_creator(postgres_user, postgres_password, postgres_db)    

# Adding the attachment (make sure the file exists before sending the email)
attachments = [absolute_path]


# DAG definition
with DAG(
        default_args=default_args,
        dag_id='aggregates_python',
        description='Downloading given building permissions, saving in a database and generating aggregates',
        start_date=datetime(2024, 1, 8),
        schedule_interval='0 0 1 * *',
        catchup=True,
) as dag:

    zip_data_downloader_task = PythonOperator(
        task_id='zip_data_downloader_task',
        python_callable=main_of_zip_data_downloader,
        dag=dag
    )

    validation_task = PythonOperator(
        task_id='validation_task',
        python_callable=main_of_validation,
        dag=dag
    )

    unzipped_data_uploader_task = PythonOperator(
        task_id='unzipped_data_uploader',
        python_callable=main_of_unzipped_data_uploader,
        dag=dag
    )

    aggregates_creation_task = PythonOperator(
        task_id='aggregates_creation_task',
        python_callable=main_of_aggregates_creation,
        dag=dag
    )

    # Define the EmailOperator task
    mail_task = EmailOperator(
        task_id='send_email',
        to=os.environ.get('EMAIL_ADDRESS_2'),  # Use environment variable for recipient email
        subject='ETL Process Report',
        html_content=html_content,  # Set the HTML content for the email
        files=attachments,  # Attach the report file, use 'files' instead of 'attachments' if using an older version of Airflow
        dag=dag  # Associate with the DAG
    )

    
#Task dependencies
zip_data_downloader_task >> validation_task >> unzipped_data_uploader_task >> aggregates_creation_task >> mail_task
