from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
from great_expectations.dataset import PandasDataset
from sqlalchemy import create_engine
import os
import json
from great_expectations.exceptions import GreatExpectationsError
import roman

default_args = {
    'owner': 'YOUR_NAME',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=2),
}

def create_roman_set():
    roman_set = set()

    for number in range(1, 31):
        roman_number = roman.toRoman(number)
        roman_set.add(roman_number)
        
    return roman_set

def data_validation(ti):
    # Set connection parameters
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    postgres_db = os.environ.get('POSTGRES_DB')
    connection_string = f"postgresql+psycopg2://postgres123:pass123@lab3-zip_data_postgres_db-1:5432/mydatabase123"
    engine = create_engine(connection_string)

    # Execute SQL query and load results into DataFrame
    query = "SELECT * FROM public.reporting_results2020"  # Change to appropriate SQL query
    df = pd.read_sql_query(query, engine)

    # Transform DataFrame into PandasDataset
    dataset = PandasDataset(df)

    # Define expectations
    dataset.expect_column_values_to_match_regex('data_wplywu_wniosku_do_urzedu', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    dataset.expect_column_values_to_be_unique('id')
    rom_set = create_roman_set()
    dataset.expect_column_values_to_be_in_set('kategoria', rom_set)
    dataset.expect_column_values_to_not_be_unique('stan') #checking if the column contains at least one other position than "brak sprzeciwu"
    column_names = [
    'id', 'numer_ewidencyjny_system', 'numer_ewidencyjny_urzad', 
    'data_wplywu_wniosku_do_urzedu', 'nazwa_organu', 'wojewodztwo_objekt', 
    'obiekt_kod_pocztowy', 'miasto', 'terc', 'cecha', 'cecha2', 
    'ulica', 'ulica_dalej', 'nr_domu', 'kategoria', 'nazwa_zam_budowlanego', 
    'rodzaj_zam_budowlanego', 'kubatura', 'stan', 'jednostki_numer', 
    'obreb_numer', 'numer_dzialki', 'numer_arkusza_dzialki', 'nazwisko_projektanta', 
    'imie_projektanta', 'projektant_numer_uprawnien', 'projektant_pozostali'
    ]
    for column in column_names:
        dataset.expect_column_values_to_not_be_null(column)

    # Validate and save results
    try:
        results = dataset.validate()
        results_json = json.dumps(
            results, 
            indent=2, 
            default=str  # Convert dates and other non-standard types to string
        )

        # Save results to file
        with open('/opt/airflow/data/validation_results.json', 'w') as f:
            f.write(results_json)

        # Save expectation suite to JSON file
        with open('/opt/airflow/data/expectation_suite.json', 'w') as f:
            f.write(json.dumps(dataset.get_expectations_config(), indent=2))

    except GreatExpectationsError as e:
        print(f"An error occurred during validation: {e}")

with DAG(
        default_args=default_args,
        dag_id='download_permissions2',
        description='Downloading given building permissions',
        start_date=datetime(2023, 6, 1),
        schedule_interval='@once'
) as dag:

    data_validation_task = PythonOperator(
        task_id='data_validation_task',
        python_callable=data_validation,
        dag=dag
    )

# Add other DAG tasks and define the appropriate dependencies

data_validation_task
