import logging
import os
import sys
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
import roman
from airflow.utils.email import send_email
from google.cloud import bigquery
from great_expectations.dataset import PandasDataset
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from pandas_gbq import to_gbq

# Use the absolute path if you need to navigate from the current working directory
absolute_path = '/opt/airflow/data/validation_results.html'

def download_and_unpack_zip(url, local_zip_path, extract_to_folder):
    """Download and unpack a ZIP file."""
    logging.info("Starting the ZIP file downloading...")
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)
    logging.info("The ZIP file downloaded. The unpacking process has been started...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    logging.info("The unpacking process has been finished")

def validation(file_path):
    # Read data from CSV into a pandas DataFrame
    df = pd.read_csv(file_path, delimiter='#', encoding='ISO-8859-2')
    
    df['terc'] = df['terc'].apply(lambda x: str(int(x)) if pd.notnull(x) and str(x).replace('.0', '').isdigit() else str(x))
    
    # Convert pandas DataFrame to Great Expectations PandasDataset for validation
    dataset = PandasDataset(df)

    # Define expectations
    rom_set = create_roman_set()
    dataset.expect_column_values_to_match_regex('data_wplywu_wniosku_do_urzedu', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    dataset.expect_column_values_to_be_in_set('kategoria', rom_set)
    dataset.expect_column_values_to_match_regex('terc', r'^\d{7}$')

    # Validate and save results
    validation_results = dataset.validate()
    renderer = ValidationResultsPageRenderer()
    rendered_page = renderer.render(validation_results)
    html = DefaultJinjaPageView().render(rendered_page)

    #Checking the directory status by logging.info
    logging.info(f"Current working directory: {os.getcwd()}")

    # Convert to the absolute path for HTML output
    absolute_path_html = '/opt/airflow/data/validation_results.html'

    # Save the validation results to an HTML file
    with open(absolute_path_html, 'w') as html_file:
        html_file.write(html)


# Utility functions
def create_roman_set():
    """Create a set of Roman numerals."""
    roman_set = set()
    for number in range(1, 31):
        roman_number = roman.toRoman(number)
        roman_set.add(roman_number)
    return roman_set

def load_data_from_csv_to_db(file_path,  ti=None, **kwargs):
    """Load data from a CSV file into the database."""
    logging.info("Connecting the database...")

    client = bigquery.Client()
    table_id = "airflow-lab-415614.airflow_dataset.reporting_results2020"

    query = """
    SELECT EXISTS(
        SELECT 1 FROM `airflow-lab-415614.airflow_dataset.reporting_results2020` LIMIT 1
    )
    """
    query_job = client.query(query) 
    results = query_job.result()

    for row in results:
        table_has_records = row[0]

    mode = 'update' if table_has_records else 'full'
    logging.info(f"mode value is {mode}")

    if mode == 'update':
        today_date_before_convertion = kwargs.get('execution_date', datetime.utcnow())
        if isinstance(today_date_before_convertion, datetime):
            today_date_str = today_date_before_convertion.strftime('%Y-%m-%dT%H:%M:%S')
            today_date = convert_iso_to_standard_format(today_date_str)
            today_date_minus_month = get_first_day_of_previous_month(today_date)
        else:
            today_date = convert_iso_to_standard_format(today_date_before_convertion)
            today_date_minus_month = get_first_day_of_previous_month(today_date)
        if today_date is None or today_date_minus_month is None:
            logging.critical("Critical Error: 'today_date' or 'today_date_minus_month' is None. Exiting program.", file=sys.stderr)
            raise Exception("InvalidDateError: Either 'today_date' or 'today_date_minus_month' has failed to be set properly.")
    else:
        today_date = 0
        today_date_before_convertion = 0
        today_date_minus_month = 0

    logging.info(f"CAUTION: before the date convertion, <today_date> has a value: {today_date_before_convertion} ")
    logging.info(f"CAUTION: current value of the <today_date> parameter is {today_date}")
    logging.info(f"CAUTION: current value of the <today_date_minus_month> parameter is {today_date_minus_month}")

    filtered_df = filter_data_on_date(file_path, today_date_minus_month)

    insert_permissions_to_db(filtered_df)

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
        logging.error(f"Error converting date: {e}")
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

def insert_permissions_to_db(df):
    """Load data from a Pandas DataFrame into the database in batches after filtering out null dates."""

    batch_size=1000

    # Establish a BigQuery connection
    client = bigquery.Client()
    table_id = "airflow-lab-415614.airflow_dataset.reporting_results2020"
    shorter_table_id = "airflow_dataset.reporting_results2020"

    table = client.get_table(table_id)

    table_schema = table.schema


    # Filter out rows where 'data_wplywu_wniosku_do_urzedu' is NaN
    df_filtered = df.dropna(subset=['data_wplywu_wniosku_do_urzedu'])

    # Calculate the number of batches
    num_batches = (len(df_filtered) + batch_size - 1) // batch_size  # Ceiling division

    # Load filtered data in batches
    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = start_index + batch_size
        df_batch = df_filtered.iloc[start_index:end_index]
        
        #PREVIOUS Version
        # Load the batch into the database
        # job = client.load_table_from_dataframe(df_batch, table_id)
        # job.result()

        #CURRENT Version
        # Load the batch into the database
    
        df_batch['data_wplywu_wniosku_do_urzedu'] = df_batch['data_wplywu_wniosku_do_urzedu'].astype(str)

        schema_dicts = [{'name': field.name, 'type': field.field_type, 'mode': field.mode} for field in table_schema]

        to_gbq(df_batch, destination_table=shorter_table_id, if_exists='append', table_schema=schema_dicts)

        logging.info(f"Loaded batch {batch_num + 1} of {num_batches} to the database")

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
    df['data_wplywu_wniosku_do_urzedu'] = convert_column_to_date(df, 'data_wplywu_wniosku_do_urzedu')
    filtered_df = df
    if min_date_str != 0:
        min_date = convert_text_to_date(min_date_str)      
        filtered_df = df[df['data_wplywu_wniosku_do_urzedu'] > min_date]
    
    return filtered_df

def convert_column_to_date(df, column_name):
    converted = pd.to_datetime(df[column_name], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    successful_conversions = converted.notna().sum()
    converted = converted.where(converted.notna(), None)
    failed_conversions = len(converted) - successful_conversions
    logging.info(f"Successful conversions: {successful_conversions}, Failed conversions: {failed_conversions}, Total conversions: {len(converted)}")
    return converted


def convert_text_to_date(text_date):
    """Convert text to datetime object."""
    if pd.isnull(text_date):
        return None
    
    try:
        converted_date = datetime.strptime(str(text_date), '%Y-%m-%d %H:%M:%S')
        return converted_date
    except ValueError:
        logging.info(f"Unsuccessful conversion in <convert_text_to_date> function")
        return 
    
def superior_aggregates_creator():

    client = bigquery.Client()

    # Execute SQL query to select records from the last 3 months
    query = """
    SELECT *
    FROM `airflow-lab-415614.airflow_dataset.reporting_results2020`
    WHERE DATE(TIMESTAMP(data_wplywu_wniosku_do_urzedu)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH);
    """

    query_job = client.query(query)
    df = query_job.to_dataframe()

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

    logging.info(f"Aggregate has been saved to: {path_to_save}")

    #Code to load the CSV file into BigQuery
    dataset_id = 'airflow-lab-415614.airflow_dataset'  # Your dataset name
    table_id = 'new_aggregate_table'  # Name of the new table in BigQuery
    table_ref = dataset_id + '.' + table_id
    
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # BigQuery will automatically detect the schema of the data.
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the first row of the file, assuming it contains headers
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Existing data in the table will be replaced with each load
    )
    
    with open(path_to_save, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config,
        )
    
    load_job.result()  # Waits for the loading to complete

    logging.info(f"Loaded {load_job.output_rows} rows into {table_ref} in BigQuery.")

    

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

def email_callback():
    send_email(
        to=[
            'jakbiel1@gmail.com'
        ],
        subject='ETL Process Report complete',
        html_content = """
        <p>Dear User,</p>
        <p>Please find attached the ETL process report generated by our system.</p>
        <p>This report contains important information regarding the data validation process, including any inconsistencies found, data quality scores, and other relevant details.</p>
        <p>For a detailed analysis, please refer to the attached HTML report.</p>
        <p>Best Regards,</p>
        <p>Your Data Processing Team</p>
        """,
        files = ['/opt/airflow/data/validation_results.html']
    )