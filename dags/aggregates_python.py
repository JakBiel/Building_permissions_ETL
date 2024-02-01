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
import requests
import zipfile
import psycopg2
from psycopg2.extras import execute_values

default_args = {
    'owner': 'YOUR_NAME',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=2),
}

# Utility functions
def create_roman_set():
    """Create a set of Roman numerals."""
    roman_set = set()
    for number in range(1, 31):
        roman_number = roman.toRoman(number)
        roman_set.add(roman_number)
    return roman_set

def convert_text_to_date(text_date):
    """Convert text to datetime object."""
    try:
        return datetime.strptime(text_date, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None

# Data processing and validation functions
def filter_data_on_date(file_path, min_date_str):
    """Filter CSV data based on a minimum date."""
    min_date = convert_text_to_date(min_date_str)
    df = pd.read_csv(file_path, delimiter='#')
    df['data_wplywu_wniosku_do_urzedu'] = df['data_wplywu_wniosku_do_urzedu'].apply(convert_text_to_date)
    filtered_df = df[df['data_wplywu_wniosku_do_urzedu'] >= min_date]
    return filtered_df

def sort_data_by_date(df):
    """Sort DataFrame by date."""
    return df.sort_values(by='data_wplywu_wniosku_do_urzedu')

def apply_additional_criteria(df, db_params):
    """Apply additional criteria to data and insert into the database."""
    batch = []
    batch_size = 1000  # Adjust as needed
    batch_iterator = 0
    all_data = []  # Store all data for new DataFrame

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Define the column names for the new DataFrame
    columns = [
        'numer_ewidencyjny_system', 'numer_ewidencyjny_urzad', 'data_wplywu_wniosku_do_urzedu', 
        'nazwa_organu', 'wojewodztwo_objekt', 'obiekt_kod_pocztowy', 'miasto', 'terc', 'cecha', 'cecha2',
        'ulica', 'ulica_dalej', 'nr_domu', 'kategoria', 'nazwa_zam_budowlanego', 'rodzaj_zam_budowlanego', 
        'kubatura', 'stan', 'jednostki_numer', 'obreb_numer', 'numer_dzialki', 
        'numer_arkusza_dzialki', 'nazwisko_projektanta', 'imie_projektanta', 
        'projektant_numer_uprawnien', 'projektant_pozostali'
    ]

    for row in df.itertuples(index=False):
        data_tuple = (
            row.numer_ewidencyjny_system, row.numer_ewidencyjny_urzad, row.data_wplywu_wniosku_do_urzedu, 
            row.nazwa_organu, row.wojewodztwo_objekt, row.obiekt_kod_pocztowy, row.miasto, row.terc, row.cecha, row.cecha2,
            row.ulica, row.ulica_dalej, row.nr_domu, row.kategoria, row.nazwa_zam_budowlanego, row.rodzaj_zam_budowlanego, 
            row.kubatura, row.stan, row.jednostki_numer, row.obreb_numer, row.numer_dzialki, 
            row.numer_arkusza_dzialki, row.nazwisko_projektanta, row.imie_projektanta, 
            row.projektant_numer_uprawnien, row.projektant_pozostali
        )
        batch.append(data_tuple)
        all_data.append(data_tuple)

        if len(batch) >= batch_size:
            execute_values(cursor, """
                INSERT INTO reporting_results2020 (
                    numer_ewidencyjny_system, numer_ewidencyjny_urzad, data_wplywu_wniosku_do_urzedu, 
                    nazwa_organu, wojewodztwo_objekt, obiekt_kod_pocztowy, miasto, terc, cecha, cecha2,
                    ulica, ulica_dalej, nr_domu, kategoria, nazwa_zam_budowlanego, rodzaj_zam_budowlanego, 
                    kubatura, stan, jednostki_numer, obreb_numer, numer_dzialki, 
                    numer_arkusza_dzialki, nazwisko_projektanta, imie_projektanta, 
                    projektant_numer_uprawnien, projektant_pozostali
                ) VALUES %s
            """, batch)
            conn.commit()
            print(f"WCZYTANO do bazy danych cały Batch o numerze: {batch_iterator}, liczba rekordów: {len(batch)}")
            batch = []
            batch_iterator += 1

    if batch:
        execute_values(cursor, """
            INSERT INTO reporting_results2020 (...) VALUES %s
        """, batch)
        conn.commit()
        print(f"WCZYTANO OSTATNI Batch do bazy danych, numer: {batch_iterator}, liczba rekordów: {len(batch)}")

    df1 = pd.DataFrame(all_data, columns=columns)
    cursor.close()
    conn.close()
    return df1

# Airflow task functions
def data_validation(validated_df):
    """Validate data using Great Expectations."""
    dataset = PandasDataset(validated_df)
    rom_set = create_roman_set()
    dataset.expect_column_values_to_match_regex('data_wplywu_wniosku_do_urzedu', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    dataset.expect_column_values_to_be_unique('id')
    dataset.expect_column_values_to_be_in_set('kategoria', rom_set)
    # Validate and save results here

def download_and_unpack_zip(url, local_zip_path, extract_to_folder):
    """Download and unpack a ZIP file."""
    print("Rozpoczynanie pobierania pliku ZIP...")
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)
    print("Plik ZIP pobrany. Rozpoczynanie rozpakowywania...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    print("Rozpakowywanie zakończone.")

def load_data_from_csv_to_db(file_path, db_params):
    """Load data from a CSV file into the database."""
    print("Łączenie z bazą danych...")
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    cursor.execute("SELECT EXISTS(SELECT 1 FROM reporting_results2020 LIMIT 1)")
    table_has_records = cursor.fetchone()[0]
    mode = 'update' if table_has_records else 'full'

    if mode == 'update': 
        cursor.execute("SELECT max(data_wplywu_wniosku_do_urzedu) FROM reporting_results2020")
        latest_date = cursor.fetchone()[0].strftime('%Y-%m-%d %H:%M:%S')

    filtered_df = filter_data_on_date(file_path, latest_date)
    sorted_df = sort_data_by_date(filtered_df)
    validation_df = apply_additional_criteria(sorted_df, db_params)

    cursor.close()
    conn.close()
    return validation_df

def main_of_unzipped_data_uploader_and_validation(ti):
    # Path to the CSV file containing data to be validated
    csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2020_up.csv'

    # Database connection parameters
    db_params = {
        'host': "zip_data_postgres_db",
        'db_name': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD')
    }

    # Initialize a database connection
    conn = psycopg2.connect(
        dbname=db_params['db_name'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host']
    )

    # Pass 'conn' as an argument to the 'load_data_from_csv_to_db' function
    df_to_be_validated = load_data_from_csv_to_db(csv_file_path, db_params)

    # Pass 'conn' as an argument to the 'data_validation' function
    data_validation(df_to_be_validated, conn)


def main_of_zip_data_downloader(ti):
    """Main function for ZIP data downloader."""
    url = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia_2020_up.zip'
    local_zip_path = 'zip_data.zip'
    extract_to_folder = 'unpacked_zip_data_files'
    download_and_unpack_zip(url, local_zip_path, extract_to_folder)

def main_of_aggregates_creation(ti):
    """Main function for aggregates creation."""
    # Set connection parameters
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    postgres_db = os.environ.get('POSTGRES_DB')
    
    HOST='host.docker.internal'
    engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{HOST}:5432/{postgres_db}')
    
    # Execute SQL query and load results into DataFrame
    query = "SELECT * FROM public.reporting_results2020"  # Change to appropriate SQL query
    df = pd.read_sql_query(query, engine)
    # Aggregate creation logic here

# DAG definition
with DAG(
        default_args=default_args,
        dag_id='aggregates_python',
        description='Downloading given building permissions, saving in a database and generating aggregates',
        start_date=datetime(2023, 6, 1),
        schedule_interval='@once'
) as dag:

    zip_data_downloader_task = PythonOperator(
        task_id='zip_data_downloader_task',
        python_callable=main_of_zip_data_downloader,
        dag=dag
    )

    uzipped_data_uploader_and_validation_task = PythonOperator(
        task_id='uzipped_data_uploader_and_validation_task',
        python_callable=main_of_unzipped_data_uploader_and_validation,
        dag=dag
    )

    # aggregates_creation_task = PythonOperator(
    #     task_id='aggregates_creation_task',
    #     python_callable=main_of_aggregates_creation,
    #     dag=dag
    # )

# Task dependencies
zip_data_downloader_task >> uzipped_data_uploader_and_validation_task #>> aggregates_creation_task
