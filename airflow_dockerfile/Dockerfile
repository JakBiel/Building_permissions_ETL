FROM apache/airflow:2.0.2

# Set environment variables for database
ARG POSTGRES_USER
ARG POSTGRES_PASSWORD
ARG POSTGRES_DB

ENV POSTGRES_USER=${POSTGRES_USER}
ENV POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
ENV POSTGRES_DB=${POSTGRES_DB}

RUN pip install pandas sqlalchemy psycopg2-binary great-expectations==0.15.0 roman
