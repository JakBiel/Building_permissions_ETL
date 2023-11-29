FROM python:3.8

WORKDIR /zip_data_downloader_app

COPY zip_data_downloader.py /zip_data_downloader_app

RUN pip install requests psycopg2-binary

CMD ["python", "zip_data_downloader.py"]
