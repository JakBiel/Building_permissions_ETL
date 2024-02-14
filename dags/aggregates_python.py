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

    #print("Connecting to the database...")
    conn = psycopg2.connect(**db_params)
    #print("Connection established.")
    
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
                
                #print(f"Starting to process batch {batch_iterator}...")
                manage_batch(cursor, conn, batch, batch_size, batch_iterator)
                #print(f"Batch {batch_iterator} processing completed.")

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
    #print(f"Loading batch {batch_iterator} to the database...")
    #print(f"Loading batch {batch_iterator} to the database with {len(batch)} records...")
    #for i, record in enumerate(batch):
    #    print(f"Record {i}: {record}")

    try:
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
        #print("Batch executed successfully")
    except Exception as e:
        print(f"Error executing batch: {e}")
    #print("Pre-ready - batch number: {batch_iterator}, number of records: {len(batch)}")
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

    #DEBUGGING PRINTS:
    #print(f"UWAGA: przed konwersją, latest_date ma wartość: {latest_date_before_convertion} ")
    #print(f"UWAGA: aktualna wartość latest_date wynosi: {latest_date}")

    #print("Starting to filter data on date...")
    filtered_df = filter_data_on_date(file_path, latest_date)
    #print("Data filtering completed.")

    #print("Starting to apply additional criteria...")
    validation_df = apply_additional_criteria(filtered_df, db_params)
    #print("Applying additional criteria completed.")

    #print(f"UWAGA: liczba zliczonych, źle skonwertowanych dat: {wrong_text_to_date_iterator}")

    cursor.close()
    conn.close()
    return validation_df

def main_of_unzipped_data_uploader_and_validation(ti):
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
    df_to_be_validated = load_data_from_csv_to_db(csv_file_path, db_params, ti)

    # Pass 'conn' as an argument to the 'data_validation' function
    data_validation(df_to_be_validated)

    ti.xcom_push(key='last_success_date', value=datetime.utcnow().isoformat())

    print("Data upload and validation completed.")

def main_of_zip_data_downloader(ti):
    """Main function for ZIP data downloader."""
    url = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia_2022_up.zip'
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

    # Execute SQL query to select records from the last 3 months
    query = """
    SELECT * FROM public.reporting_results2020
    WHERE data_wplywu_wniosku_do_urzedu::timestamp >= current_date - INTERVAL '3 months';
    """
    df = pd.read_sql_query(query, engine)

    df['data_wplywu_wniosku_do_urzedu'] = pd.to_datetime(df['data_wplywu_wniosku_do_urzedu'], errors='coerce')
    df['terc'] = pd.to_numeric(df['terc'], errors='coerce').fillna(0).astype(int)


    # Aggregate creation logic here

    # Prepare date-related data
    today = pd.Timestamp(datetime.now())

    # Convert 'data_wplywu_wniosku_do_urzedu' to datetime and filter data for the last 3, 2, and 1 month(s)
    df_last_3m = df
    df_last_2m = df[df['data_wplywu_wniosku_do_urzedu'] >= today - pd.DateOffset(months=2)]
    df_last_1m = df[df['data_wplywu_wniosku_do_urzedu'] >= today - pd.DateOffset(months=1)]

    aggregate3m = aggregate_creator(df_last_3m, "3m")
    aggregate2m = aggregate_creator(df_last_2m, "2m")
    aggregate1m = aggregate_creator(df_last_1m, "1m")

    #TEMPORARILY COMMENTED
    # # Save the ordered DataFrame to a CSV file, ensuring 'injection_date' is the second column

    summary_aggregate = merge_aggregates(aggregate3m,aggregate2m,aggregate1m)

    final_aggregate = correct_aggregates_column_order_plus_injection_date(summary_aggregate)
    #aggregate3m1 = correct_aggregates_column_order_plus_injection_date(aggregate3m)

    path_to_save = '/opt/airflow/desktop/aggregate_result.csv'
    final_aggregate.to_csv(path_to_save, index=False)

    print(f"Aggregate has been saved to: {path_to_save}")

def aggregate_creator(df, prefix):
    aggregate = pd.pivot_table(df, index=['terc'], columns=['kategoria', 'rodzaj_zam_budowlanego'], 
                               aggfunc='size', fill_value=0)
    
    # Ensure 'terc' becomes a column if it's part of the index
    if 'terc' not in aggregate.columns:
        aggregate.reset_index(inplace=True)

    # Modify column names except for 'terc'
    if isinstance(aggregate.columns, pd.MultiIndex):
        # Flatten MultiIndex if necessary and prepend prefix
        aggregate.columns = [f"{prefix}_{'_'.join(col).rstrip('_')}" if col[0] != 'terc' else col[0] for col in aggregate.columns.values]
    else:
        # Prepend prefix to column names except for 'terc'
        aggregate.columns = [f"{prefix}_{col}" if col != 'terc' else col for col in aggregate.columns]

    return aggregate

def merge_aggregates(aggregate3m, aggregate2m, aggregate1m):

    merged_aggregate = pd.merge(aggregate3m, aggregate2m, on='terc', how='outer', suffixes=('_3m', '_2m'))
    merged_aggregate = pd.merge(merged_aggregate, aggregate1m, on='terc', how='outer')

    for col in merged_aggregate.columns:
        if col not in ['terc', 'injection_date'] and merged_aggregate[col].dtype == float:
            merged_aggregate[col] = merged_aggregate[col].fillna(0).astype(int)

    return merged_aggregate






def correct_aggregates_column_order_plus_injection_date(aggregate_0):

    # Adding 'injection_date' after ensuring 'terc' is a column
    aggregate_0['injection_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC'

    # Prepare ordered columns list with 'terc' and 'injection_date' at the beginning
    ordered_columns = ['terc', 'injection_date'] + [col for col in aggregate_0.columns if col not in ['terc', 'injection_date']]

    # Apply the ordered columns to the DataFrame
    aggregate_ordered = aggregate_0[ordered_columns]

    aggregate_ordered = aggregate_ordered.rename(columns={'terc': 'unit_id'})

    return aggregate_ordered
    


# DAG definition
with DAG(
        default_args=default_args,
        dag_id='aggregates_python',
        description='Downloading given building permissions, saving in a database and generating aggregates',
        start_date=datetime(2024, 1, 8),
        schedule_interval='0 0 1 * *',
        catchup=False,  # no catching-up if scheduled dag failed
) as dag:

    # zip_data_downloader_task = PythonOperator(
    #     task_id='zip_data_downloader_task',
    #     python_callable=main_of_zip_data_downloader,
    #     dag=dag
    # )

    # uzipped_data_uploader_and_validation_task = PythonOperator(
    #     task_id='uzipped_data_uploader_and_validation_task',
    #     python_callable=main_of_unzipped_data_uploader_and_validation,
    #     dag=dag
    # )

    aggregates_creation_task = PythonOperator(
        task_id='aggregates_creation_task',
        python_callable=main_of_aggregates_creation,
        dag=dag
    )

# Task dependencies
#zip_data_downloader_task >> uzipped_data_uploader_and_validation_task >> aggregates_creation_task
aggregates_creation_task