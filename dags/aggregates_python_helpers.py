import logging
import os
import shutil
import sys
import zipfile
from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
import requests
import roman
from airflow.utils.email import send_email
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
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

def validate_permissions_data(file_path):
    # Read data from CSV into a pandas DataFrame
    df = pd.read_csv(file_path, delimiter='#', encoding='ISO-8859-2')
    
    df['terc'] = df['terc'].apply(lambda x: str(int(x)) if pd.notnull(x) and str(x).replace('.0', '').isdigit() else str(x))
    
    # Convert pandas DataFrame to Great Expectations PandasDataset for validation
    dataset = PandasDataset(df)

    # Preparation of expected values in the "rodzaj_zam_budowlanego" column
    expected_types = ['budowa nowego/nowych obiektów budowlanych', 'rozbudowa istniejącego/istniejących obiektów budowlanych', 'odbudowa istniejącego/istniejących obiektów budowlanych', 'nadbudowa istniejącego/istniejących obiektów budowlanych']

    # Define expectations
    rom_set = create_roman_set()
    dataset.expect_column_values_to_match_regex('data_wplywu_wniosku_do_urzedu', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    dataset.expect_column_values_to_be_in_set('kategoria', rom_set)
    dataset.expect_column_values_to_match_regex('terc', r'^\d{7}$')
    dataset.expect_column_distinct_values_to_be_in_set('rodzaj_zam_budowlanego', expected_types)

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

def load_permissionss_to_bq(file_path, params, **kwargs):

    create_and_configure_bigquery_db(params)

    """Load data from a CSV file into BigQuery."""
    table_id = params['table_id']

    client = bigquery.Client()
    # Checking if the table already has records
    query = f"SELECT EXISTS(SELECT 1 FROM `{table_id}` LIMIT 1)"
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
    
    # Subtract one month from the current date
    previous_month_date = current_date.replace(day=1) - timedelta(days=1)

    # Get the first day of the previous month
    first_day_of_previous_month = previous_month_date.replace(day=1)

    # Format the result as a string in the same format as the input
    return first_day_of_previous_month.strftime('%Y-%m-%d %H:%M:%S')

def insert_permissions_to_db(df):
    """Load data from a Pandas DataFrame into the database in batches after filtering out null dates."""

    batch_size=10000

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

    # Zliczanie unikalnych wartości w kolumnie 'kolumna'
    liczba_unikalnych = df_filtered['data_wplywu_wniosku_do_urzedu'].nunique()
    print("Liczba unikalnych wartości:", liczba_unikalnych)


    # Load filtered data in batches
    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = start_index + batch_size
        df_batch = df_filtered.iloc[start_index:end_index]
        
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
    
def superior_aggregates_creator(params):
    """Function to create aggregates."""
    dataset_id = params['dataset_id']
    project_id = params['project_id']
    table_id_for_aggregates = f"{dataset_id}.new_aggregate_table"

    client = bigquery.Client()

    query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.reporting_results2020`
    WHERE DATE(TIMESTAMP(data_wplywu_wniosku_do_urzedu)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH);
    """

    query_job = client.query(query)
    df = query_job.to_dataframe()

    df['data_wplywu_wniosku_do_urzedu'] = pd.to_datetime(df['data_wplywu_wniosku_do_urzedu'], errors='coerce')
    df['terc'] = pd.to_numeric(df['terc'], errors='coerce').fillna(0).astype(int)

    # Prepare date-related data
    today = pd.Timestamp(datetime.now())

    # Filter data for the last 3, 2, and 1 month(s)
    df_last_3m = df
    df_last_2m = df[df['data_wplywu_wniosku_do_urzedu'] >= today - pd.DateOffset(months=2)]
    df_last_1m = df[df['data_wplywu_wniosku_do_urzedu'] >= today - pd.DateOffset(months=1)]

    # Create aggregates
    aggregate3m = aggregate_creator(df_last_3m, "3m")
    aggregate2m = aggregate_creator(df_last_2m, "2m")
    aggregate1m = aggregate_creator(df_last_1m, "1m")

    # Merge and correct aggregates
    summary_aggregate = merge_aggregates(aggregate3m, aggregate2m, aggregate1m)
    final_aggregate = correct_aggregates_column_order_plus_injection_date(summary_aggregate)

    # Clean column names to meet BigQuery requirements
    final_aggregate.columns = [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in final_aggregate.columns]

    # Ensure column names do not start with digits
    final_aggregate.columns = ['_' + col if col[0].isdigit() else col for col in final_aggregate.columns]

    # Directly save the DataFrame to BigQuery
    final_aggregate.to_gbq(table_id_for_aggregates, project_id=project_id, if_exists='replace', progress_bar=True)

    # Log a message to indicate successful save operation
    logging.info(f"DataFrame has been successfully saved to BigQuery, table: {table_id_for_aggregates}.")
    

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

def email_callback(params):
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

def create_and_configure_bigquery_db(params):
    # Logging the value of dataset_id
    # Attempting to retrieve 'dataset_id' from params; if not found, defaults to 'Brak dataset_id'
    dataset_id = params['dataset_id']
    # Logging the retrieved 'dataset_id' to help verify its correctness
    logging.info(f"dataset_id: {dataset_id}")

    # Check if the retrieved 'dataset_id' is indeed a string
    if not isinstance(dataset_id, str):
        # If not, raise an error specifying the expected type
        raise ValueError(f"dataset_id should be a string, received: {type(dataset_id)}")
    
    # Retrieving 'project_id' from params for further use in BigQuery operations
    project_id = params['project_id']
    # Further logic for configuring BigQuery would go here, now with a verified dataset_id

    table_id = params['table_id_name']
    
    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct references for the dataset and table
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # Define table schema
    schema = [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("numer_ewidencyjny_system", "STRING"),
        bigquery.SchemaField("numer_ewidencyjny_urzad", "STRING"),
        bigquery.SchemaField("data_wplywu_wniosku_do_urzedu", "DATE"),
        bigquery.SchemaField("nazwa_organu", "STRING"),
        bigquery.SchemaField("wojewodztwo_objekt", "STRING"),
        bigquery.SchemaField("obiekt_kod_pocztowy", "STRING"),
        bigquery.SchemaField("miasto", "STRING"),
        bigquery.SchemaField("terc", "STRING"),
        bigquery.SchemaField("cecha", "STRING"),
        bigquery.SchemaField("cecha2", "STRING"),
        bigquery.SchemaField("ulica", "STRING"),
        bigquery.SchemaField("ulica_dalej", "STRING"),
        bigquery.SchemaField("nr_domu", "STRING"),
        bigquery.SchemaField("kategoria", "STRING"),
        bigquery.SchemaField("nazwa_zam_budowlanego", "STRING"),
        bigquery.SchemaField("rodzaj_zam_budowlanego", "STRING"),
        bigquery.SchemaField("kubatura", "STRING"),
        bigquery.SchemaField("stan", "STRING"),
        bigquery.SchemaField("jednostki_numer", "STRING"),
        bigquery.SchemaField("obreb_numer", "STRING"),
        bigquery.SchemaField("numer_dzialki", "STRING"),
        bigquery.SchemaField("numer_arkusza_dzialki", "STRING"),
        bigquery.SchemaField("nazwisko_projektanta", "STRING"),
        bigquery.SchemaField("imie_projektanta", "STRING"),
        bigquery.SchemaField("projektant_numer_uprawnien", "STRING"),
        bigquery.SchemaField("projektant_pozostali", "STRING")
    ]

    # Try to create the dataset (if it does not exist)
    try:
        client.get_dataset(dataset_ref)
        print("Dataset already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")

    # Try to create the table (if it does not exist)
    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        # Set monthly partitioning on 'data_wplywu_wniosku_do_urzedu' column
        table.time_partitioning = bigquery.TimePartitioning(
            type_="MONTH",
            field="data_wplywu_wniosku_do_urzedu"  # The field to partition by
        )
        # Set clustering fields right before creating the table
        table.clustering_fields = ['terc']
        # Only create the table once with all configurations set
        table = client.create_table(table)
        logging.info(f"Created table {table_id} with monthly partitioning and clustering on 'terc' in BigQuery")



    load_shapefile_to_bigquery(params)    

def load_shapefile_to_bigquery(params):
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # URL of the zip file to download
    url = 'https://www.gis-support.pl/downloads/2022/powiaty.zip'
    local_zip_path = '/tmp/powiaty.zip'
    extract_to_folder = '/tmp/powiaty'

    # Downloading the zip file
    logging.info("Downloading zip file...")
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)

    # Extracting the zip file
    logging.info("Extracting zip file...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)

    # Checking if the table exists in BigQuery
        
    project_id = params['project_id']
    dataset_id = params['dataset_id']

    client = bigquery.Client(project=project_id)
    table_id = 'powiaty'
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_id} already exists in BigQuery. No action taken.")
    except NotFound:
        shapefile_path = f'{extract_to_folder}/powiaty.shp'
        gdf = gpd.read_file(shapefile_path)
        
        if 'geometry' in gdf.columns:
            gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt)
            schema = [bigquery.SchemaField(name=column, field_type='STRING') for column in gdf.columns]
            
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logging.info(f"Created table {table_id} in BigQuery.")

            job = client.load_table_from_dataframe(gdf, table_ref)
            job.result()
            logging.info(f"Loaded {job.output_rows} rows into {table_id}.")
        else:
            logging.warning("GeoDataFrame does not contain a geometry column.")

    # Cleaning up the downloaded zip file and the extracted folder
    os.remove(local_zip_path)
    shutil.rmtree(extract_to_folder)
    logging.info("Cleaned up downloaded and extracted files.")