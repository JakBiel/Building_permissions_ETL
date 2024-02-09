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

wrong_text_to_date_iterator = 0

def convert_text_to_date(text_date):
    """Convert text to datetime object."""
    global wrong_text_to_date_iterator
    if pd.isnull(text_date):
        wrong_text_to_date_iterator = wrong_text_to_date_iterator + 1
        return None
    try:
        return datetime.strptime(str(text_date), '%Y-%m-%d %H:%M:%S')
    except ValueError:
        wrong_text_to_date_iterator = wrong_text_to_date_iterator + 1
        return None


# Data processing and validation functions
def filter_data_on_date(file_path, min_date_str):
    """Filter CSV data based on a minimum date."""
    column_names = [
        'numer_ewidencyjny_system', 'numer_ewidencyjny_urzad', 'data_wplywu_wniosku_do_urzedu', 
        'nazwa_organu', 'wojewodztwo_objekt', 'obiekt_kod_pocztowy', 'miasto', 'terc', 'cecha', 'cecha2',
        'ulica', 'ulica_dalej', 'nr_domu', 'kategoria', 'nazwa_zam_budowlanego', 'rodzaj_zam_budowlanego', 
        'kubatura', 'stan', 'jednostki_numer', 'obreb_numer', 'numer_dzialki', 
        'numer_arkusza_dzialki', 'nazwisko_projektanta', 'imie_projektanta', 
        'projektant_numer_uprawnien', 'projektant_pozostali'
    ]

    df = pd.read_csv(file_path, delimiter='#', names=column_names, header=0)
    df['data_wplywu_wniosku_do_urzedu'] = df['data_wplywu_wniosku_do_urzedu'].apply(convert_text_to_date)
    filtered_df = df
    if min_date_str != 0:
        min_date = convert_text_to_date(min_date_str)      
        filtered_df = df[df['data_wplywu_wniosku_do_urzedu'] >= min_date]
    
    return filtered_df

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

    print("MY_INFO: The correct date records are being uploaded to the database...")

    for row in df.itertuples(index=False):
        # Check if 'data_wplywu_wniosku_do_urzedu' is not NaN before adding to batch
        if not pd.isna(row.data_wplywu_wniosku_do_urzedu):
            batch.append(row_to_tuple(row))
            all_data.append(row_to_tuple(row))
            if len(batch) >= batch_size:
                manage_batch(cursor, conn, batch, batch_size, batch_iterator)
                batch.clear()
                batch_iterator += 1
        
    # Final batch insert if any records are left
    if batch:
        execute_values(cursor, """
            INSERT INTO reporting_results2020 (
                numer_ewidencyjny_system, numer_ewidencyjny_urzad, data_wplywu_wniosku_do_urzedu, 
                nazwa_organu, wojewodztwo_objekt, obiekt_kod_pocztowy, miasto, terc, cecha, cecha2,
                ulica, ulica_dalej, nr_domu, kategoria, nazwa_zam_budowlanego, rodzaj_zam_budowlanego, 
                kubatura, stan, jednostki_numer, obreb_numer, numer_dzialki, 
                numer_arkusza_dzialki, nazwisko_projektanta, imie_projektanta, 
                projektant_numer_uprawnien, projektant_pozostali
            ) VALUES %s
        """, batch, page_size=batch_size)
        conn.commit()
        print(f"Loaded the FINAL batch to the database, batch number: {batch_iterator}, number of records: {len(batch)}")

    df1 = pd.DataFrame(all_data, columns=columns)
    cursor.close()
    conn.close()

    return df1

def row_to_tuple(row):
    """Convert DataFrame row to tuple suitable for database insertion."""
    return (
        row.numer_ewidencyjny_system, row.numer_ewidencyjny_urzad, 
        row.data_wplywu_wniosku_do_urzedu,
        row.nazwa_organu, row.wojewodztwo_objekt, row.obiekt_kod_pocztowy, 
        row.miasto, row.terc, row.cecha, row.cecha2, row.ulica, 
        row.ulica_dalej, row.nr_domu, row.kategoria, row.nazwa_zam_budowlanego, 
        row.rodzaj_zam_budowlanego, row.kubatura, row.stan, row.jednostki_numer, 
        row.obreb_numer, row.numer_dzialki, row.numer_arkusza_dzialki, 
        row.nazwisko_projektanta, row.imie_projektanta, 
        row.projektant_numer_uprawnien, row.projektant_pozostali
    )

def manage_batch(cursor, conn1, batch, batch_size, batch_iterator):
    """Manage batch insertions to the database."""
    execute_values(cursor, """
        INSERT INTO reporting_results2020 (
            numer_ewidencyjny_system, numer_ewidencyjny_urzad, data_wplywu_wniosku_do_urzedu, 
            nazwa_organu, wojewodztwo_objekt, obiekt_kod_pocztowy, miasto, terc, cecha, cecha2,
            ulica, ulica_dalej, nr_domu, kategoria, nazwa_zam_budowlanego, rodzaj_zam_budowlanego, 
            kubatura, stan, jednostki_numer, obreb_numer, numer_dzialki, 
            numer_arkusza_dzialki, nazwisko_projektanta, imie_projektanta, 
            projektant_numer_uprawnien, projektant_pozostali
        ) VALUES %s
    """, batch, page_size=batch_size)
    conn1.commit()
    print(f"Loaded a full batch to the database, batch number: {batch_iterator}, number of records: {len(batch)}")



# Airflow task functions
def data_validation(validated_df):
    """Validate data using Great Expectations."""
    dataset = PandasDataset(validated_df)
    rom_set = create_roman_set()
    dataset.expect_column_values_to_match_regex('data_wplywu_wniosku_do_urzedu', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
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

def load_data_from_csv_to_db(file_path, db_params, ti):
    """Load data from a CSV file into the database."""
    print("Łączenie z bazą danych...")
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    cursor.execute("SELECT EXISTS(SELECT 1 FROM reporting_results2020 LIMIT 1)")
    table_has_records = cursor.fetchone()[0]
    mode = 'update' if table_has_records else 'full'

    if mode == 'update': 
        latest_date_before_convertion = ti.xcom_pull(task_ids='uzipped_data_uploader_and_validation_task', key='last_success_date')
        latest_date = convert_text_to_date(ti.xcom_pull(task_ids='uzipped_data_uploader_and_validation_task', key='last_success_date'))
        if latest_date is None:
            latest_date = 0
            latest_date_before_convertion = 0 
            cursor.execute("TRUNCATE TABLE reporting_results2020 RESTART IDENTITY;") 
    else:
        latest_date = 0
        latest_date_before_convertion = 0

    print(f"UWAGA: przed konwersją, latest_date ma wartość: {latest_date_before_convertion} ")
    print(f"UWAGA: aktualna wartość latest_date wynosi: {latest_date}")
    filtered_df = filter_data_on_date(file_path, latest_date)
    validation_df = apply_additional_criteria(filtered_df, db_params)

    print(f"UWAGA: liczba zliczonych, źle skonwertowanych dat: {wrong_text_to_date_iterator}")

    cursor.close()
    conn.close()
    return validation_df

def main_of_unzipped_data_uploader_and_validation(ti):
    # Path to the CSV file containing data to be validated
    csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2020_up.csv'

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
    df_to_be_validated = load_data_from_csv_to_db(csv_file_path, db_params, ti)

    # Pass 'conn' as an argument to the 'data_validation' function
    data_validation(df_to_be_validated)

    ti.xcom_push(key='last_success_date', value=datetime.utcnow().isoformat())

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
        start_date=datetime(2024, 1, 8),
        schedule_interval='0 0 1 * *',
        catchup=False,  # no catching-up if scheduled dag failed
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
