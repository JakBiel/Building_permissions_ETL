from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
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
import sys

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
correct_text_to_date_iterator = 0

def convert_text_to_date(text_date):
    """Convert text to datetime object."""
    global wrong_text_to_date_iterator
    global correct_text_to_date_iterator
    if pd.isnull(text_date):
        wrong_text_to_date_iterator += 1
        return None
    
    try:
        converted_date = datetime.strptime(str(text_date), '%Y-%m-%d %H:%M:%S')
        correct_text_to_date_iterator += 1
        return converted_date
    except ValueError:
        wrong_text_to_date_iterator += 1
        return None


#Especially for Apache Airflow metadata dates convertion
def convert_iso_to_standard_format(iso_date_str):
    """Convert an ISO 8601 date string to 'YYYY-MM-DD HH:MM:SS' format for Python versions older than 3.7."""
    try:
        # Parse the ISO 8601 string to a datetime object manually
        # Note: This assumes the input is always in 'YYYY-MM-DDTHH:MM:SS.ssssss' format
        date_obj = datetime.strptime(iso_date_str.split('.')[0], '%Y-%m-%dT%H:%M:%S')
        
        # Format the datetime object back to a string in 'YYYY-MM-DD HH:MM:SS' format
        standard_date_str = date_obj.strftime('%Y-%m-%d %H:%M:%S')
        
        return standard_date_str
    except ValueError as e:
        print(f"Error converting date: {e}")
        return None
    
def get_first_day_of_previous_month(date_str):
    """Returns the first day of the month before the given date."""
    # Convert the string to datetime type
    current_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    year = current_date.year
    month = current_date.month
    
    # Decrement the month by one
    if month == 1:  # January
        month = 12  # December
        year -= 1  # Previous year
    else:
        month -= 1  # Previous month

    # Set day to the first of the month
    new_date = datetime(year, month, 1)
    return new_date.strftime('%Y-%m-%d %H:%M:%S')  # Return the new date as a string in the same format


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
        filtered_df = df[df['data_wplywu_wniosku_do_urzedu'] > min_date]
    
    return filtered_df

def insert_permissions_to_db(df, db_params):
    """Load data from a Pandas DataFrame into the database in batches after filtering out null dates."""

    batch_size=1000

    # Create a database engine
    engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}/{db_params['dbname']}")

    # Filter out rows where 'data_wplywu_wniosku_do_urzedu' is NaN
    df_filtered = df.dropna(subset=['data_wplywu_wniosku_do_urzedu'])

    # Calculate the number of batches
    num_batches = (len(df_filtered) + batch_size - 1) // batch_size  # Ceiling division

    # Load filtered data in batches
    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = start_index + batch_size
        df_batch = df_filtered.iloc[start_index:end_index]
        
        # Load the batch into the database
        df_batch.to_sql('reporting_results2020', engine, if_exists='append', index=False, method='multi')
        
        print(f"Loaded batch {batch_num + 1} of {num_batches} to the database")

    # Close the database engine
    engine.dispose()


def validation(file_path):
    # Read data from CSV into a pandas DataFrame
    df = pd.read_csv(file_path, delimiter='#', encoding='ISO-8859-2')
    
    # Convert pandas DataFrame to Great Expectations PandasDataset for validation
    dataset = PandasDataset(df)

    # Define expectations
    rom_set = create_roman_set()
    dataset.expect_column_values_to_match_regex('data_wplywu_wniosku_do_urzedu', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    dataset.expect_column_values_to_be_in_set('kategoria', rom_set)

    # Validate and save results
    validation_results = dataset.validate()
    validation_results_json = validation_results.to_json_dict()

    #Debug print
    print(f"Current working directory: {os.getcwd()}")

    # Convert to the absolute path
    absolute_path = '/opt/airflow/data/validation_results.json'
    #Debug print
    print(f"Absolute path: {absolute_path}")  # Print the absolute path

    # Check if the directory exists
    #Debug prints
    directory_path = os.path.dirname(absolute_path)
    if not os.path.exists(directory_path):
        print(f"Directory does not exist: {directory_path}")
    else:
        print(f"Directory exists: {directory_path}")


    # Save the validation results to a JSON file
    with open(absolute_path, 'w') as json_file:
        json.dump(validation_results_json, json_file)

def download_and_unpack_zip(url, local_zip_path, extract_to_folder):
    """Download and unpack a ZIP file."""
    print("Starting the ZIP file downloading...")
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)
    print("The ZIP file downloaded. The unpacking process has been started...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    print("The unpacking process has been finished")

def load_data_from_csv_to_db(file_path, db_params, ti=None, **kwargs):
    """Load data from a CSV file into the database."""
    print("Connecting the database...")
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    cursor.execute("SELECT EXISTS(SELECT 1 FROM reporting_results2020 LIMIT 1)")
    table_has_records = cursor.fetchone()[0]
    mode = 'update' if table_has_records else 'full'
    print(f"mode value is {mode}")

    if mode == 'update':
        today_date_before_convertion = kwargs.get('execution_date', datetime.utcnow())
        if isinstance(today_date_before_convertion, datetime):
            today_date_str = today_date_before_convertion.strftime('%Y-%m-%dT%H:%M:%S')
            today_date = convert_iso_to_standard_format(today_date_str)
        else:
            today_date = convert_iso_to_standard_format(today_date_before_convertion)
        today_date_minus_month = get_first_day_of_previous_month(today_date)
        if today_date is None or today_date_minus_month is None:
            print("Critical Error: 'today_date' or 'today_date_minus_month' is None. Exiting program.", file=sys.stderr)
            raise Exception("InvalidDateError: Either 'today_date' or 'today_date_minus_month' has failed to be set properly.")
    else:
        today_date = 0
        today_date_before_convertion = 0

    print(f"CAUTION: before the date convertion, <today_date> has a value: {today_date_before_convertion} ")
    print(f"CAUTION: current value of the <today_date> parameter is {today_date}")
    print(f"CAUTION: current value of the <today_date_minus_month> parameter is {today_date_minus_month}")

    filtered_df = filter_data_on_date(file_path, today_date_minus_month)

    insert_permissions_to_db(filtered_df, db_params)

    print(f"CAUTION: the number of the counted incorrectly-converted dates: {wrong_text_to_date_iterator}")
    print(f"CAUTION: the number of the counted CORRECTLY-converted dates: {correct_text_to_date_iterator}")
    print(f"CAUTION: the GENERAL number of the counted converted dates [both, incorrectly and correctly]: {correct_text_to_date_iterator + wrong_text_to_date_iterator}")

    cursor.close()
    conn.close()

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

    print("Data upload and validation completed.")

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

    summary_aggregate = merge_aggregates(aggregate3m,aggregate2m,aggregate1m)

    final_aggregate = correct_aggregates_column_order_plus_injection_date(summary_aggregate)

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
    

# Use the absolute path if you need to navigate from the current working directory
absolute_path = '/opt/airflow/data/validation_results.json'

# Creating more elaborate HTML content for the email
html_content = """
<p>Dear User,</p>
<p>Please find attached the ETL process report generated by our system.</p>
<p>This report contains important information regarding the data validation process, including any inconsistencies found, data quality scores, and other relevant details.</p>
<p>For a detailed analysis, please refer to the attached JSON report.</p>
<p>Best Regards,</p>
<p>Your Data Processing Team</p>
"""

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
