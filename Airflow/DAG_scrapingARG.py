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
#URL Argentina
ARG_science = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=data%20science&location=Argentina&geoId=100446943&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start='
ARG_engineer = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=data%20engineer&location=Argentina&geoId=100446943&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start='
ARG_analyst='https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=data%20analyst&location=Argentina&geoId=100446943&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start='
ARG = [ARG_analyst,ARG_engineer,ARG_science]

#Definimos las listas que despues van a formar el DF
puesto = []
empresa = []
descripcion = []
fecha = []

#DAG que hace scraping y lo guarda en google storage
def scraping():
    #Iteracion para el scraping
    c = 0 #variable para elegir puesto
    for p in ARG:
        for i in range(0,1000,25):#Recorre multiples paginas
            response = requests.get(p + str(i))#A partir de la iteracion se agrega al final del URL indica que pagina hace scraping
            #Levanta la pagina y extrae los datos(puesto de trabajo, empresa, fehca, link de publicacion)
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
                #Del link de publicacion se vuelve a hacer scraping y se obtiene la descripcion del trabajo
                link = job.find('a', class_='base-card__full-link')['href']
                response = requests.get(link)
                soup = BeautifulSoup(response.content,'html.parser')
                descripcion_bool = soup.find('div', class_="show-more-less-html__markup show-more-less-html__markup--clamp-after-5")
                if type(descripcion_bool) == type(job):
                    descripcion.append(descripcion_bool.text)
                else:
                    descripcion.append(None)
        c = c + 1
    sleep(20)#Tiene un delay de 20seg para que la pagina no nos fuerce la salida
    #se crea el dataframe y se guarda en la nube
    df = pd.DataFrame()
    df['Fecha'] = fecha
    df['DescripcionTrabajo'] = descripcion
    df['salario'] = None
    df['pais'] = 'ARG'
    df['NombreEmpresa'] = empresa
    df['Modalidad'] = None
    df['Plataforma'] = 'linkedin'
    df['PuestoTrabajo'] = puesto

    #Guardamos en Cloud Storage
    df.to_csv('gs://proyecto-final-data/Scraping/scrapingArg.csv',index=False)

#instanciamos los pipeline y el orden de flujo
with DAG(
    dag_id = 'ScrapingARG',
    start_date=datetime(2023, 1, 19),
    catchup=False,
    schedule_interval = '0 12 * * 1'
) as dag:

    t_begin = DummyOperator(task_id="begin")

    Scrap = PythonOperator(
        task_id = 'scrapingARG',
        python_callable = scraping,
        dag=dag
    )

    t_end = DummyOperator(task_id='end')

t_begin>>Scrap>>t_end