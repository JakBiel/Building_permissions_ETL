FROM jupyter/base-notebook

WORKDIR /zip_data_jupyter_notebook

COPY . .

RUN pip install -r requirements.txt

RUN pip install pandas numpy matplotlib seaborn psycopg2-binary unidecode

WORKDIR /zip_data_jupyter_notebook/jpdata