import requests
import zipfile
import os
import psycopg2
from psycopg2.extras import execute_values
import csv
import sys
from datetime import datetime, timedelta
import argparse

def download_and_unpack_zip(url, local_zip_path, extract_to_folder):
    print("Rozpoczynanie pobierania pliku ZIP...")
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)
    print("Plik ZIP pobrany. Rozpaczynanie rozpakowywania...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    print("Rozpakowywanie zakończone.")

def check_if_empty(db_params):
    conn = psycopg2.connect(
        dbname=db_params['db_name'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host']
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM reporting_results2020")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count == 0

def get_last_month():
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.strftime("%Y-%m")

def convert_text_to_date(text_date):
    return datetime.strptime(text_date.split(' ')[0], '%Y-%m-%d')

def load_data_from_csv(file_path, db_params, mode):
    print("Łączenie z bazą danych...")
    conn = psycopg2.connect(
        dbname=db_params['db_name'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host']
    )
    print("Połączenie z bazą danych nawiązane.")
    cursor = conn.cursor()

    csv.field_size_limit(sys.maxsize)

    print("Rozpoczynanie wczytywania danych z CSV...")
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='#')
        next(reader)  # Skip the header

        batch = []
        batch_iterator = 0
        last_month = get_last_month() if mode == "update" else None

        for row in reader:
            if len(row) != 26 or any(len(value) > 131071 for value in row):
                continue  # Pomijanie wierszy z błędnymi danymi

            if mode == "update":
                record_date = convert_text_to_date(row[2])
                if record_date.strftime("%Y-%m") != last_month:
                    continue

            batch.append(tuple(row))
            print(f"Wczytano do batcha rekord nr: {batch_iterator}.{len(batch)}")

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
                print(f"WCZYTANO do b.danych cały Batch o nr-ze: {batch_iterator}, liczba jego rekordów: {len(batch)}")
                batch = []
                batch_iterator += 1

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
            print(f"WCZYTANO OSTATNI Batch do b.danych, ma on nr: {batch_iterator}, liczba jego rekordów: {len(batch)}")

    cursor.close()
    conn.close()
    print("Wczytywanie danych zakończone.")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Download and process ZIP data.')
    parser.add_argument('--mode', type=str, default='full', choices=['full', 'update'],
                        help='Tryb uruchamiania skryptu: "full" dla pełnego załadowania danych, "update" dla aktualizacji danych.')
    return parser.parse_args()

def main():
    args = parse_arguments()

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

    if args.mode == 'full':
        if check_if_empty(db_params):
            print("Baza danych jest pusta. Rozpoczynanie pełnego załadowania danych.")
            download_and_unpack_zip(url, local_zip_path, extract_to_folder)
            load_data_from_csv(csv_file_path, db_params, args.mode)
        else:
            print("Baza danych nie jest pusta. Zakończenie działania programu.")
            return

    elif args.mode == 'update':
        print("Rozpoczynanie aktualizacji danych z ostatniego miesiąca.")
        download_and_unpack_zip(url, local_zip_path, extract_to_folder)
        load_data_from_csv(csv_file_path, db_params, args.mode)

if __name__ == "__main__":
    print("Rozpoczynanie głównego procesu...")
    main()
    print("Zakończono główny proces.")
