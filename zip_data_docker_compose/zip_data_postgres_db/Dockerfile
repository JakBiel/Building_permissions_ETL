# Use the official PostgreSQL image as a base
FROM postgres:latest

# Set environment variables for database
ARG POSTGRES_USER
ARG POSTGRES_PASSWORD
ARG POSTGRES_DB

ENV POSTGRES_USER=${POSTGRES_USER}
ENV POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
ENV POSTGRES_DB=${POSTGRES_DB}

# Optionally, you can add scripts to be run on container initialization
COPY ./init.sql /docker-entrypoint-initdb.d/

# The container will run PostgreSQL on the default port
EXPOSE 5432
