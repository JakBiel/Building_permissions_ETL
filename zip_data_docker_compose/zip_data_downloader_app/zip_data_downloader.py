import requests
import zipfile
import os
import psycopg2
import csv
import sys


url = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia_2020_up.zip'
response = requests.get(url)

# Saving the file locally
with open('zip_data.zip', 'wb') as file:
    file.write(response.content)

# Unpacking the .zip file
with zipfile.ZipFile('zip_data.zip', 'r') as zip_ref:
    zip_ref.extractall('unpacked_zip_data_files')


# Connection parameters
host = "zip_data_postgres_db"
db_name = os.environ.get('POSTGRES_DB')
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')

def check_duplicate(cursor, row):
    # Check if a record with the same value in the first column already exists
    cursor.execute("SELECT * FROM reporting_results2020 WHERE numer_ewidencyjny_system = %s", (row[0],))
    existing_record = cursor.fetchone()

    if existing_record:
        # If a record is found, compare all columns
        return all([existing_record[i] == row[i] for i in range(len(row))])
    else:
        # If no record is found, it's not a duplicate
        return False

def load_data_from_csv(file_path):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, 
        user=user, 
        password=password, 
        host=host
    )
    cursor = conn.cursor()


    # Set the maximum field size to the maximum integer value
    csv.field_size_limit(sys.maxsize)

    # Open the CSV file
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='#')

        # Skip the first row (header)
        next(reader)

        iterator = 0
        iterator_of_bad = 0

        for row in reader:
            if len(row) != 26:  # Ensure the row has exactly 26 columns
                if len(row) > 2:
                    print(f"Warning: row with incorrect number of columns: {len(row)}", row)
                print(f"----{iterator_of_bad}")
                iterator_of_bad=iterator_of_bad+1
                continue

            row_is_valid = True  # flag showing if the row is correct

            for value in row:
                if len(value) > 131071:
                    print(f"Zbyt długa wartość, liczba znaków:{len(value)} dla kolumny: ", row, " ", value)
                    row_is_valid = False  # flag setting as False if the row is invalid
                    break
                

            if not row_is_valid:  # Checking if the row is correct. If not, a new iteraion of "for row in reader" is beginning
                print(f"----{iterator_of_bad}")
                iterator_of_bad=iterator_of_bad+1
                continue

            print(f"{iterator}")
            iterator=iterator+1

            if check_duplicate(cursor, row):
                print(f"Duplicate record found, skipping insertion")
                continue

            cursor.execute("""
                INSERT INTO reporting_results2020 (
                    numer_ewidencyjny_system, numer_ewidencyjny_urzad, data_wplywu_wniosku_do_urzedu, 
                    nazwa_organu, wojewodztwo_objekt, obiekt_kod_pocztowy, miasto, terc, cecha, cecha2,
                    ulica, ulica_dalej, nr_domu, kategoria, nazwa_zam_budowlanego, rodzaj_zam_budowlanego, 
                    kubatura, stan, jednostki_numer, obreb_numer, numer_dzialki, 
                    numer_arkusza_dzialki, nazwisko_projektanta, imie_projektanta, 
                    projektant_numer_uprawnien, projektant_pozostali
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, row)


    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

# Replace with your CSV file path
csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2020_up.csv'
load_data_from_csv(csv_file_path)
