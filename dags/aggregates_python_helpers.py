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

def validate_permissions_data(file_path, html_path):
    # Read data from CSV into a pandas DataFrame
    df = pd.read_csv(file_path, delimiter='#', encoding='UTF-8')
    
    df['terc'] = df['terc'].apply(lambda x: str(int(x)) if pd.notnull(x) and str(x).replace('.0', '').isdigit() else str(x))
    
    # Convert pandas DataFrame to Great Expectations PandasDataset for validation
    dataset = PandasDataset(df)

    # Preparation of expected values in the "rodzaj_zam_budowlanego" column
    expected_types = ['budowa nowego/nowych obiektów budowlanych', 'rozbudowa istniejącego/istniejących obiektów budowlanych', 'odbudowa istniejącego/istniejących obiektów budowlanych', 'nadbudowa istniejącego/istniejących obiektów budowlanych', 'wykonanie robót budowlanych innych niż wymienione powyżej']

    # Define expectations
    rom_set = create_roman_set()

    # Checking if the dates in 'data_wplywu_wniosku_do_urzedu' column have the correct format
    dataset.expect_column_values_to_match_regex('data_wplywu_wniosku_do_urzedu', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    # Checking if the values in 'kategoria' column are the expected numbers of the roman type
    dataset.expect_column_values_to_be_in_set('kategoria', rom_set)
    # Checking if the 'terc' column includes the correct type (6 or 7 digits) of TERC codes; test failes when >15% records are invalid
    dataset.expect_column_values_to_match_regex('terc', r'^\d{6,7}$', mostly=0.85)
    # Checking if the 'rodzaj_zam_budowlanego' column includes all 4 expected types of building construction intention
    dataset.expect_column_distinct_values_to_be_in_set('rodzaj_zam_budowlanego', expected_types)

    # Validate and save results
    validation_results = dataset.validate()
    renderer = ValidationResultsPageRenderer()
    rendered_page = renderer.render(validation_results)
    html = DefaultJinjaPageView().render(rendered_page)

    #Checking the directory status by logging.info
    logging.info(f"Current working directory: {os.getcwd()}")

    # Save the validation results to an HTML file
    with open(html_path, 'w') as html_file:
        html_file.write(html)


# Utility functions
def create_roman_set():
    """Create a set of Roman numerals."""
    roman_set = set()
    for number in range(1, 31):
        roman_number = roman.toRoman(number)
        roman_set.add(roman_number)
    return roman_set

def load_permissions_to_bq(file_path, params, **kwargs):

    gdf_powiaty = create_and_configure_bigquery_db(params)

    """Load data from a CSV file into BigQuery."""
    table_id = f"{params['project_id']}.{params['dataset_id']}.{params['table_id_name']}"

    client = bigquery.Client()
    # Checking if the table already has records
    query = f"SELECT EXISTS(SELECT 1 FROM `{table_id}` LIMIT 1)"
    query_job = client.query(query) 
    results = query_job.result()

    for row in results:
        table_has_records = row[0]

    mode = 'update' if table_has_records else 'full'
    logging.info(f"mode value is {mode}")

    today_date_minus_month = pd.NA

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
        
    logging.info(f"CAUTION: current value of the <today_date_minus_month> parameter is {today_date_minus_month}")

    filtered_df = filter_data_on_date(file_path, today_date_minus_month)

    corrected_data_df = data_correction_on_invalid_terc_codes(filtered_df, gdf_powiaty)

    insert_permissions_to_db(corrected_data_df, params)

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

def insert_permissions_to_db(df, params):
    """Load data from a Pandas DataFrame into the database in batches after filtering out null dates."""

    batch_size=10000

    # Establish a BigQuery connection
    client = bigquery.Client()
    table_id = f"{params['project_id']}.{params['dataset_id']}.{params['table_id_name']}"
    shorter_table_id = f"{params['dataset_id']}.{params['table_id_name']}"

    table = client.get_table(table_id)

    table_schema = table.schema


    # Filter out rows where 'data_wplywu_wniosku_do_urzedu' is NaN
    df_filtered = df.dropna(subset=['data_wplywu_wniosku_do_urzedu'])

    # Calculate the number of batches
    num_batches = (len(df_filtered) + batch_size - 1) // batch_size  # Ceiling division

    # Counting the unique values in the column 'data_wplywu_wniosku_do_urzedu'
    liczba_unikalnych = df_filtered['data_wplywu_wniosku_do_urzedu'].nunique()
    logging.info(f"Number of unique values: {liczba_unikalnych}")


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
    if not pd.isna(min_date_str):
        min_date = convert_text_to_date(min_date_str)      
        filtered_df = df[df['data_wplywu_wniosku_do_urzedu'] > min_date]
        logging.info("There was a filtration of data by dates")
    else:
        logging.info("There was NO filtration of data by dates")

    return filtered_df

def data_correction_on_invalid_terc_codes(df1, gdf1):
    df1['terc'] = df1['terc'].apply(lambda x: str(int(x)) if pd.notnull(x) and str(x).replace('.0', '').isdigit() else str(x))

    new_columns = df1.apply(assign_terc_and_validate, args=(gdf1,), axis=1)

    df1['terc'] = new_columns['terc']
    df1['untypical'] = new_columns['untypical']

    general_initial_rows_number = len(df1)
    generally_unknown_terc_codes_number = len(df1.loc[df1['untypical'] == 'Unknown'])
    generally_unknown_terc_codes_number2 = len(df1.loc[df1['untypical'] == 'Unknown2'])
    generally_unknown_terc_codes_number3 = len(df1.loc[df1['untypical'] == 'Unknown3'])
    correct_terc_codes = general_initial_rows_number - generally_unknown_terc_codes_number - generally_unknown_terc_codes_number2 -  generally_unknown_terc_codes_number3

    logging.info(f"Number of all rows in the Dataframe before terc codes correction: {general_initial_rows_number}")
    logging.info(f"Number of rows with 'untypical' value 'Unknown': {generally_unknown_terc_codes_number}")
    logging.info(f"Number of rows with 'untypical' value 'Unknown2': {generally_unknown_terc_codes_number2}")
    logging.info(f"Number of rows with 'untypical' value 'Unknown3': {generally_unknown_terc_codes_number3}")

    logging.info(f"Number of correct powiat records (after data correction): {correct_terc_codes}")
    logging.info(f"Percentage of invalid records to be removed in the next steps: {round((general_initial_rows_number - correct_terc_codes)/general_initial_rows_number * 100, 2)}%")

    df1 = df1[df1['untypical'] != 'Unknown']
    df1 = df1[df1['untypical'] != 'Unknown2']
    df1 = df1[df1['untypical'] != 'Unknown3']

    logging.info(f"Invalid record are removed")

    # Removing column "untypical" that is no longer needed in the df1 after the logging messages above and removing unvalidated records
    df1 = df1.drop('untypical', axis=1)

    # Cleaning up the downloaded zip file and the extracted folder with <powiaty.shp> data as it is no longer needed.
    local_zip_path = '/tmp/powiaty.zip'
    extract_to_folder = '/tmp/powiaty'

    os.remove(local_zip_path)
    shutil.rmtree(extract_to_folder)
    logging.info("Cleaned up downloaded and extracted files with <powiaty.shp> data as it is no longer needed")

    return df1

def assign_terc_and_validate(row, gdf1):
    # Dictionary with mappings of voivodeship codes to voivodeship names
    voivodeships = {
        '02': 'Lower Silesian',
        '04': 'Kuyavian-Pomeranian',
        '06': 'Lublin',
        '08': 'Lubusz',
        '10': 'Lodz',
        '12': 'Lower Poland',
        '14': 'Masovian',
        '16': 'Opole',
        '18': 'Podkarpackie',
        '20': 'Podlaskie',
        '22': 'Pomeranian',
        '24': 'Silesian',
        '26': 'Swietokrzyskie',
        '28': 'Warmian-Masurian',
        '30': 'Greater Poland',
        '32': 'West Pomeranian',
    }

    terc_code = row['terc']
    untypical = None  # a new variable for new statuses like 'Unknown' or 'Unknown2'
    jednostki_numer = row['jednostki_numer']

    if not terc_code or pd.isnull(terc_code) or terc_code == 'nan':  # If terc code is empty
        jednostki_numer = row['jednostki_numer']
        if pd.notnull(jednostki_numer) and jednostki_numer != 'nan':
            terc_code = jednostki_numer[:4]
            untypical = 'Matched'
        else:
            miasto = row['miasto']
            if pd.notnull(miasto) and miasto != 'nan':
                matching_rows = gdf1[gdf1['JPT_NAZWA_'].str.contains(miasto, case=False, na=False)]
            else:
                matching_rows = pd.DataFrame()
            
            if not matching_rows.empty:
                matching_row = matching_rows.iloc[0]
                terc_code = matching_row['JPT_KOD_JE']
                untypical = 'Matched'
            else:
                terc_code = 'Unknown'
                untypical = 'Unknown'

    if len(str(terc_code)) == 7 and str(terc_code).isdigit(): #checking if terc_code is of a typical terc_code type
        if terc_code[:2] not in voivodeships.keys():
            untypical = 'Unknown2' 
    elif len(str(terc_code)) == 6 and str(terc_code).isdigit():  # checking if the terc_code consists of 6 digits and is a number so it could be fixed with setting '0' at the beginning
        terc_code = '0' + str(terc_code) 
        if terc_code[:2] not in voivodeships.keys():
            untypical = 'Unknown2'
    elif len(str(terc_code)) == 4 and str(terc_code).isdigit() and untypical == 'Matched':
        pass
    elif untypical == 'Unknown':
        pass
    else:
        untypical = 'Unknown3'

    
    return pd.Series({'terc': terc_code, 'untypical': untypical})

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
    FROM `{project_id}.{dataset_id}.permissions_results2022`
    WHERE DATE(TIMESTAMP(data_wplywu_wniosku_do_urzedu)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH);
    """

    query_job = client.query(query)
    df = query_job.to_dataframe()

    df['data_wplywu_wniosku_do_urzedu'] = pd.to_datetime(df['data_wplywu_wniosku_do_urzedu'], errors='coerce')
    df['terc'] = df['terc'].str.slice(0, 4)


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
    print("Before summary_aggregate")
    summary_aggregate = merge_aggregates(aggregate3m, aggregate2m, aggregate1m)
    print("Before final_aggregate")
    final_aggregate = correct_aggregates_column_order_plus_injection_date(summary_aggregate)

    print("Before query_powiaty creation")
    #Temporary download of powiaty list for additional validation of the final aggregate
    query_powiaty = f"""
    SELECT JPT_KOD_JE
    FROM `{project_id}.{dataset_id}.powiaty`;
    """
    print("Before query_job_powiaty")
    query_job_powiaty = client.query(query_powiaty)
    powiaty_df = query_job_powiaty.to_dataframe()
    
    final_aggregate_after_removals = removing_false_records_from_aggregate(final_aggregate,powiaty_df)
    final_aggregate = adding_empty_records_for_powiats_with_zero_permissions(final_aggregate_after_removals, powiaty_df)

    # Clean column names to meet BigQuery requirements
    final_aggregate.columns = [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in final_aggregate.columns]

    # Ensure column names do not start with digits
    final_aggregate.columns = ['_' + col if col[0].isdigit() else col for col in final_aggregate.columns]

    # Directly save the DataFrame to BigQuery
    final_aggregate.to_gbq(table_id_for_aggregates, project_id=project_id, if_exists='append', progress_bar=True)

    # Log a message to indicate successful save operation
    logging.info(f"DataFrame has been successfully saved to BigQuery, table: {table_id_for_aggregates}.")
    

def aggregate_creator(df, prefix):
    aggregate = pd.pivot_table(df, index=['terc'], columns=['rodzaj_zam_budowlanego', 'kategoria'], 
                               aggfunc='size', fill_value=0)

    # Ensure 'terc' becomes a column if it's part of the index
    if 'terc' not in aggregate.columns:
        aggregate.reset_index(inplace=True)

    # Modify column names except for 'terc'
    if isinstance(aggregate.columns, pd.MultiIndex):
        # Flatten MultiIndex if necessary and append prefix
        aggregate.columns = [f"{'_'.join(col).rstrip('_')}_{prefix}" if col[0] != 'terc' else col[0] for col in aggregate.columns.values]
    else:
        # Append prefix to column names except for 'terc'
        aggregate.columns = [f"{col}_{prefix}" if col != 'terc' else col for col in aggregate.columns]

    shorter_aggregate_column_names(aggregate)
    # deromanize_categories_numbers(aggregate)
    
    return aggregate

def shorter_aggregate_column_names(aggregate):
    """Modify column names in the aggregate DataFrame in place."""
    
    # Define the prefixes to match
    prefixes = ['budo', 'rozb', 'odbu', 'nadb']

    # Initialize list to keep track of columns to drop
    columns_to_drop = []

    # Iterate over each column by its index and name
    for index, col in enumerate(aggregate.columns):
        # Check if the column starts with any of the prefixes
        if any(col.startswith(prefix) for prefix in prefixes):
            # Extract the first word (prefix) and keep the content after the first underscore
            first_word = col.split(' ')[0]
            suffix_index = col.find('_')
            if suffix_index != -1:
                # Directly modify the column name in the DataFrame
                new_name = f"{first_word}{col[suffix_index:]}"
                aggregate.columns.values[index] = new_name
        elif col.startswith('terc'):
            # Keep the column unchanged if it starts with 'terc'
            continue
        else:
            # Mark the column for removal if it doesn't meet the criteria
            columns_to_drop.append(col)

    # Remove columns that don't meet the criteria
    aggregate.drop(columns=columns_to_drop, inplace=True)

    return aggregate

# def deromanize_categories_numbers(aggregate):
#     """Replace Roman numerals between underscores with Arabic numerals prefixed by 'kat_'."""
#     for index in range(len(aggregate.columns)):
#         col_name = aggregate.columns[index]
#         print(f"Processing column: {col_name}")

#         # Split the column name by underscores
#         parts = col_name.split('_')
#         if len(parts) >= 3:
#             try:
#                 roman_numeral = parts[1]
#                 arabic_number = str(roman.fromRoman(roman_numeral))
#                 parts[1] = f"kat_{arabic_number}"
#                 aggregate.columns.values[index] = '_'.join(parts)
#                 print(f"Converted column: {aggregate.columns.values[index]}")
#             except roman.InvalidRomanNumeralError:
#                 print(f"Invalid Roman numeral: {roman_numeral}, in column: {col_name}")
#                 continue

#     return aggregate

def merge_aggregates(aggregate3m, aggregate2m, aggregate1m):

    print("Before first merge")
    print("Nazwy kolumn dla aggregate3m:")
    print(aggregate3m.columns)

    print("\nNazwy kolumn dla aggregate2m:")
    print(aggregate2m.columns)

    merged_aggregate = pd.merge(aggregate3m, aggregate2m, on='terc', how='outer', validate='one_to_one')

    print("Before second merge")
    merged_aggregate = pd.merge(merged_aggregate, aggregate1m, on='terc', how='outer')

    print("Before loop")
    for col in merged_aggregate.columns:
        print("Before something in loop")
        if col not in ['terc', 'injection_date'] and merged_aggregate[col].dtype == float:
            print("Before fillna")
            merged_aggregate[col] = merged_aggregate[col].fillna(0).astype(int)

    print("Before return in merge")
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

def removing_false_records_from_aggregate(final_aggregate, powiaty_df):
    # Merge final_aggregate with powiaty_df on 'unit_id' and 'JPT_KOD_JE'
    merged_df = final_aggregate.merge(powiaty_df, how='left', left_on='unit_id', right_on='JPT_KOD_JE')
    
    # Select rows where 'JPT_KOD_JE' is null, indicating no match was found
    false_records = merged_df[merged_df['JPT_KOD_JE'].isnull()]
    
    # Log the number of false records
    logging.info(f"Number of false records to be removed from the final aggregate: {len(false_records)}")
    
    # Remove false records from final_aggregate
    final_aggregate_cleaned = final_aggregate[~final_aggregate['unit_id'].isin(false_records['unit_id'])]
    
    # Return the cleaned final_aggregate DataFrame
    return final_aggregate_cleaned

def adding_empty_records_for_powiats_with_zero_permissions(final_aggregate_after_removals1, powiaty_df1):
    """
    Add empty records for powiats with zero permissions.

    Args:
    - final_aggregate_after_removals1: DataFrame containing the final aggregate after false records removal.
    - powiaty_df1: DataFrame containing powiaty data.

    Returns:
    - DataFrame: Final aggregate DataFrame with empty records added for powiats with zero permissions.
    """
    # Merge final_aggregate_after_removals1 with powiaty_df1 on 'unit_id' and 'JPT_KOD_JE'
    merged_df = final_aggregate_after_removals1.merge(powiaty_df1, how='right', left_on='unit_id', right_on='JPT_KOD_JE')
    
    # Select rows where 'unit_id' is null, indicating no match was found
    no_permission_powiats = merged_df[merged_df['unit_id'].isnull()]
    
    # Get column names and data types from merged_df
    columns_and_types = merged_df.dtypes.to_dict()
    
    # Create empty records DataFrame with the same schema as merged_df
    empty_records = pd.DataFrame(columns=columns_and_types.keys())
    
    # Fill empty_records DataFrame with zeros
    for column in empty_records.columns:
        if column == 'unit_id':
            empty_records['unit_id'] = no_permission_powiats['JPT_KOD_JE']
        elif column == 'injection_date':
            empty_records['injection_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC'
        else:
            empty_records[column] = 0
    
    # Concatenate empty records with final_aggregate_after_removals1
    final_aggregate_with_empty = pd.concat([final_aggregate_after_removals1, empty_records], ignore_index=True)
    
    # Sort final_aggregate_with_empty by 'unit_id' column
    final_aggregate_with_empty.sort_values(by='unit_id', inplace=True)

    # Remove column 'JPT_KOD_JE'
    final_aggregate_with_empty.drop(columns=['JPT_KOD_JE'], inplace=True, errors='ignore')

    return final_aggregate_with_empty



def email_callback(params):
    recipient_email = os.getenv('EMAIL_ADDRESS_2')
    if recipient_email is None:
        raise ValueError("!!! EMAIL_RECIPIENT is not set! The mail cannot be sent !!!")

    send_email(
        to=[recipient_email],
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
        logging.info("Dataset already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset)
        logging.info(f"Created dataset {dataset_id}")

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


    gdf_file1 = load_shapefile_to_bigquery(params)    

    return gdf_file1

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


    # Creation of GeoDataFrame "gdf" with powiaty.shp data
    shapefile_path = f'{extract_to_folder}/powiaty.shp'
    gdf = gpd.read_file(shapefile_path)
    gdf = gdf.to_crs(4326)
    if 'geometry' in gdf.columns:
        gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt)
        schema = [bigquery.SchemaField(name=column, field_type='STRING') for column in gdf.columns]
    else:
        logging.warning("GeoDataFrame does not contain a geometry column.")


    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_id} already exists in BigQuery. No action taken.")
    # If table not found in BigQuery, the "gdf" is sent to BigQuery as a new table
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Created table {table_id} in BigQuery.")
        
        job = client.load_table_from_dataframe(gdf, table_ref)
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into {table_id}.")

    return gdf