from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json
import pandas as pd
import requests
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import storage
#Lee la api de getonboard y lo guarda en cloud storage
def read_api():
    params = dict(per_page=100)
    url = 'https://www.getonbrd.com/api/v0/categories?per_page=10&page=1'
    response = requests.get(url, params=params)
    data = response.json()
    df = pd.json_normalize(data['data'])
    df.rename(columns={'attributes.name' : 'name', 'attributes.dimension' : 'dimension'}, inplace=True)
    Client = storage.Client()
    bucket = Client.get_bucket('proyecto-final-data')
    bucket = Client.get_bucket('proyecto-final-data')
    blob = bucket.blob('data.csv')
    blob.upload_from_string(df.to_csv(), 'text/csv')

#Instanciamos el flujo de trabajo
with DAG(
    dag_id = 'Recuperar_API',
    schedule_interval = None,
    start_date=datetime(2023, 1, 19),
    catchup=False
    ) as dag:

        api_task = PythonOperator(
            task_id = 'levantar_API',
            python_callable = read_api,
            dag=dag)
        
        cargar_store = GCSToGCSOperator(
            task_id = 'upload_api',
            destination_bucket = 'pruebadeair',
            source_object = 'jobs.csv',
            move_object=True,
            dag=dag
        )

api_task>>cargar_store