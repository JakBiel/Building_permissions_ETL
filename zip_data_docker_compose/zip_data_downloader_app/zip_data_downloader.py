import requests
import zipfile
import os
import psycopg2
from psycopg2.extras import execute_values
import csv
import sys


def download_and_unpack_zip(url, local_zip_path, extract_to_folder):
    print("Rozpoczynanie pobierania pliku ZIP...")
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)
    print("Plik ZIP pobrany. Rozpoczynanie rozpakowywania...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    print("Rozpakowywanie zakończone.")


def load_data_from_csv(file_path, db_params):
    print("Łączenie z bazą danych...")
    conn = psycopg2.connect(
        dbname=db_params['db_name'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host']
    )
    print("Połączenie z bazą danych nawiązane.")
    cursor = conn.cursor()

    # Zwiększ limit rozmiaru pola
    csv.field_size_limit(sys.maxsize)

    print("Rozpoczynanie wczytywania danych z CSV...")
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='#')
        next(reader)  # Skip the header

        batch = []
        for row in reader:
            if len(row) != 26 or any(len(value) > 131071 for value in row):
                continue  # Pomijanie wierszy z błędnymi danymi

            batch.append(tuple(row))

            if len(batch) >= 1000:
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
                batch = []

        
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
            """, batch)
            conn.commit()

    cursor.close()
    conn.close()
    print("Wczytywanie danych zakończone.")


def remove_duplicates(db_params):
    print("Usuwanie duplikatów...")
    conn = psycopg2.connect(
        dbname=db_params['db_name'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host']
    )
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM reporting_results2020
        WHERE ctid IN (
            SELECT ctid
            FROM (
                SELECT ctid, ROW_NUMBER() OVER (
                    PARTITION BY numer_ewidencyjny_system
                    ORDER BY (SELECT 1)
                ) AS rn
                FROM reporting_results2020
            ) t
            WHERE rn > 1
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Duplikaty usunięte.")


def main():
    url = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia_2020_up.zip'
    local_zip_path = 'zip_data.zip'
    extract_to_folder = 'unpacked_zip_data_files'
    csv_file_path = 'unpacked_zip_data_files/wynik_zgloszenia_2020_up.csv'

    db_params = {
        'host': "zip_data_postgres_db",
        'db_name': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD')
    }

    download_and_unpack_zip(url, local_zip_path, extract_to_folder)
    load_data_from_csv(csv_file_path, db_params)
    remove_duplicates(db_params)


if __name__ == "__main__":
    print("Rozpoczynanie głównego procesu...")
    main()
    print("Zakończono główny proces.")
