from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from google.cloud import storage
import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

#Definimos las paginas de las cuales se va a hacer scraping
#URL
MX_engineer = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Data%2BEngineer&location=M%C3%A9xico&geoId=103323778&trk=public_jobs_jobs-search-bar_search-submit&start='
MX_analyst = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Data%2BAnalyst&location=M%C3%A9xico&geoId=103323778&trk=public_jobs_jobs-search-bar_search-submit&start='
MX_science = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Data%2BScientist&location=M%C3%A9xico&geoId=103323778&trk=public_jobs_jobs-search-bar_search-submit&start="
MX = [MX_analyst,MX_engineer,MX_science]

#Definimos las listas que despues van a formar el DF
puesto = []
empresa = []
descripcion = []
fecha = []

#DAG que hace scraping y lo guarda en google storage
def scraping():
    c = 0
    #Iteracion para el scraping
    for p in MX:
        for i in range(0,1000,25):
            response = requests.get(p + str(i))
            soup = BeautifulSoup(response.content,'html.parser')
            jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')
            for job in jobs:
                if c == 0:
                    puesto.append('data analyst')
                elif c == 1:
                    puesto.append('data engineer')
                else:
                    puesto.append('data scientist')

                empresa.append(job.find('h4', class_='base-search-card__subtitle').text.strip())

                fecha_bool = job.find('time', class_="job-search-card__listdate")
                if fecha_bool != None:
                    fecha.append(job.find('time', class_="job-search-card__listdate")['datetime'])
                else:
                    fecha.append(None)

                link = job.find('a', class_='base-card__full-link')['href']
                response = requests.get(link)
                soup = BeautifulSoup(response.content,'html.parser')
                descripcion_bool = soup.find('div', class_="show-more-less-html__markup show-more-less-html__markup--clamp-after-5")
                if type(descripcion_bool) == type(job):
                    descripcion.append(descripcion_bool.text)
                else:
                    descripcion.append(None)
        c = c + 1
    sleep(20)

    df = pd.DataFrame()
    df['Fecha'] = fecha
    df['DescripcionTrabajo'] = descripcion
    df['salario'] = None
    df['pais'] = 'MX'
    df['NombreEmpresa'] = empresa
    df['Modalidad'] = None
    df['Plataforma'] = 'linkedin'
    df['PuestoTrabajo'] = puesto

    #Guardamos en Cloud Storage
    df.to_csv('gs://proyecto-final-data/Scraping/scrapingMX.csv',index=False)


with DAG(
    dag_id = 'ScrapingMX',
    start_date=datetime(2023, 1, 19),
    catchup=False,
    schedule_interval = '0 18 * * 1'
) as dag:

    t_begin = DummyOperator(task_id="begin")

    Scrap = PythonOperator(
        task_id = 'scrapingMX',
        python_callable = scraping,
        dag=dag
    )

    t_end = DummyOperator(task_id='end')

t_begin>>Scrap>>t_end