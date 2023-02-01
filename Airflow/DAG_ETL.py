from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

#DAG para crear el dataframe de OfertasLaborales y subirlo a la tabla con el mismo nombre
#Se lee el archivo OfertasLaborales que se obtuvo de etl de csv y lo fusiona con los resultados del scraping
#para obtener una tabla unica
def ETL_Ofertaslaborales():
    from sqlalchemy import create_engine
    import pandas as pd
    db_username = 'prueba' #Usuario externo que hemos creado
    db_password = 'grupo05elmejor' #Contraseña del usuario externo
    db_ip = '34.151.201.62' #IP externa de la instancia
    db_name = 'data_proyect'

    s = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(db_username, db_password, db_ip, db_name)
    
    engine = create_engine(s)

    with engine.connect() as conn, conn.begin():
        ofertas_laborales = pd.read_sql('OfertasLaborales', conn)
    ofertas_laborales = ofertas_laborales.drop(columns='Unnamed: 0')

    path = 'gs://proyecto-final-data/Scraping/'
    arg = pd.read_csv(path + 'scrapingARG.csv')
    arg['Pais'] = 'Argentina'
    arg['Continente'] = 'South America'
    cl = pd.read_csv(path + 'scrapingCL.csv')
    cl['Pais'] = 'Chile'
    cl['Continente'] = 'South America'
    mx = pd.read_csv(path + 'scrapingMX.csv')
    mx['Pais'] = 'Mexico'
    mx['Continente'] = 'South America'
    co = pd.read_csv(path + 'scrapingCO.csv')
    co['Pais'] = 'Colombia'
    co['Continente'] = 'South America'
    pe = pd.read_csv(path + 'scrapingPE.csv')
    pe['Pais'] = 'Peru'
    pe['Continente'] = 'South America'

    df = pd.concat([arg,cl,mx,co,pe])
    herramienta = pd.read_csv('gs://proyecto-final-data/glasdoor_out/Herramientas.csv')

    lista_herramientas= herramienta["herramienta"].tolist()

    m= df['DescripcionTrabajo'].str.lower()
    m=pd.DataFrame(m)

    ##separar el contenido de la columna en palabras con split
    m=m['DescripcionTrabajo'].str.split()
    lista = []
    for i in m:
        guardo = []
        for p in lista_herramientas:
            if type(i) != type(2.0):
                if p in i:
                    guardo.append(p)
        guardo=list(set(guardo))
        lista.append(guardo)
    df['herramientas'] = lista
    df = df.rename(columns={'NombreEmpresa':'Empresa','salario':'Salario','Plataforma':'PlataformasEmpleo','pais':'idPais'})
    df = df.drop(columns='DescripcionTrabajo')
    df = df[['Salario','Empresa','Modalidad','PlataformasEmpleo','Fecha','PuestoTrabajo','Pais','idPais','Continente','herramientas']]
    df['Fecha'] = pd.to_datetime(df['Fecha'], format='%Y/%m/%d')

    dfinal = pd.concat([df,ofertas_laborales])
    dfinal = dfinal.drop_duplicates()
    dfinal = dfinal.reset_index(drop=True)
    dfinal.to_csv('gs://proyecto-final-data/glasdoor_out/OfertasLaborales.csv',index=False)

    dfinal = dfinal.drop(columns=['herramientas'])
    dfinal = dfinal.drop_duplicates(keep='False')
    dfinal.to_sql(name='OfertasLaborales', con=engine, if_exists='append')

    empresa_pais= ofertas_laborales[["Empresa", "idPais", "Continente"]].drop_duplicates()
    empresa_pais.to_sql(con=engine,name='Empresas_has_PaisesConContinentes', index=False, if_exists='append')
#ETL para obtener el conteo de herramientas pedidas de las descripciones de puesto de trabajo
def ETL_Herr_carrera():
    import pandas as pd
    from collections import Counter
    import ast
    import numpy as np
    from sqlalchemy import create_engine
    db_username = 'prueba' #Usuario externo que hemos creado
    db_password = 'grupo05elmejor' #Contraseña del usuario externo
    db_ip = '34.151.201.62' #IP externa de la instancia
    db_name = 'data_proyect'

    s = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(db_username, db_password, db_ip, db_name)
    
    engine = create_engine(s)

    d1 = pd.read_csv('gs://proyecto-final-data/glasdoor_out/OfertasLaborales.csv')

    herr_data_science= d1[d1["PuestoTrabajo"]=="data scientist"]
    herr_ds=herr_data_science["herramientas"].tolist()
    lista_plana = []

    for elemento in herr_ds:
        for item in elemento:
            lista_plana.append(item)
            
    lista=Counter(lista_plana)

    herr_ds=pd.DataFrame.from_dict(lista, orient="index").reset_index()
    herr_ds["PuestoTrabajo"]="data scientist"


    #data engineer
    herr_data_engineer= d1[d1["PuestoTrabajo"]=="data engineer"]
    herr_de=herr_data_engineer["herramientas"].tolist()
    lista_plana = []
    for elemento in herr_de:
        for item in elemento:
            lista_plana.append(item)

    lista=Counter(lista_plana)
    herr_de=pd.DataFrame.from_dict(lista, orient="index").reset_index()
    herr_de["PuestoTrabajo"]="data engineer"


    #data analyst
    herr_data_analyst= d1[d1["PuestoTrabajo"]=="data analyst"]
    herr_da=herr_data_analyst["herramientas"].tolist()
    lista_plana = []
    for elemento in herr_da:
        for item in elemento:
            lista_plana.append(item)

    lista=Counter(lista_plana)
    herr_da=pd.DataFrame.from_dict(lista, orient="index").reset_index()
    herr_da["PuestoTrabajo"]="data analyst"

    #unimos todo el dataframe en herramientas por carrera
    herr_carrera= pd.concat([herr_ds, herr_de, herr_da])

    #Renombramos columnas
    herr_carrera= herr_carrera.rename(columns={"index":"herramienta",
                                0:"cuenta",     
    })
    herr_carrera.to_sql(con=engine, name='herr_carrera', index=False, if_exists='replace')

#Creacion y puesta en produccion del pipeline
with DAG(
    dag_id = 'Procesamiento',
    start_date=datetime(2023, 1, 19),
    catchup=False,
    schedule_interval =None
) as dag:

    t_begin = DummyOperator(task_id="begin")

    etl0_task = PythonOperator(
        task_id = 'OfertasLaborales',
        python_callable = ETL_Ofertaslaborales,
        dag=dag
    )

    etl1_task = PythonOperator(
        task_id = 'herr_carrera',
        python_callable = ETL_Herr_carrera,
        dag=dag
    )

    t_end = DummyOperator(task_id='end')

t_begin>>etl0_task>>etl1_task>>t_end