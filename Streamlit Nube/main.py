import sys
sys.path.append("/Users/carolinasosa/opt/anaconda3/lib/python3.9/site-packages")
import streamlit as st
import streamlit.components.v1 as components
import numpy as np
import pandas as pd
import sklearn
import pickle
from pypdf import PdfReader

st.set_page_config(layout='wide')

page_bg_img = """
<style>
[data-testid="stAppViewContainer"]{
background-image : url("https://www.pexels.com/photo/blue-and-purple-cosmic-sky-956999/");
background-size : cover;
}
</style>
"""

st.markdown(page_bg_img,unsafe_allow_html=True)

st.title('Human Capital')

tab1, tab2 = st.tabs(["Insights", "Match"]) 

with tab1:
    cols = st.columns([1, 1, 1, 1, 1, 1])

    with cols[1]:
        components.html("""<iframe title="Pagina 1 - Estado mercado" width="900" height="720" src="https://app.powerbi.com/view?r=eyJrIjoiNDhkYzYxZTgtMmZjNy00OTcyLWEyYWQtMzQ5NzlkOTA0NDk4IiwidCI6IjNhNDIzMDA3LTk3MDktNGU0NS1iNGFmLTJmOTliZDM3ZDE4ZSJ9" frameborder="0" allowFullScreen="true"></iframe>""",900,720)
    with st.expander("Ver explicación"):
        st.write("""El insight "Sueldos y Ofertas laborales" muestra el promedio del salario anual por cada carrera: Data analyst, Data engineer y Data scientist. Además de elegir el año,puede filtrar por trimestre y por sub región del mundo. Se muestra en el mapa la densidad de ofertas laborales a nivel global o si selecciona una sub región se mostrará la densidad solo de ese lugar. En la parte superior derecha se encuentran los salarios promedios por año por carrera y su variación porcentual con el año anterior, y debajo el cambio en los salarios promedios por año y carrera.""")

    cols = st.columns([1, 1, 1, 1, 1, 1])

    with cols[1]:
        components.html("""<iframe title="Pagina 1 - Empresas" width="600" height="373.5" src="https://app.powerbi.com/view?r=eyJrIjoiMTZmYjFmZTctOWVkMy00ZWRhLWJkMTEtMmI0NDc0ZjVjZWZiIiwidCI6IjNhNDIzMDA3LTk3MDktNGU0NS1iNGFmLTJmOTliZDM3ZDE4ZSJ9" frameborder="0" allowFullScreen="true"></iframe>""",1280,720)
    with st.expander("Ver explicación"):
        st.write("""El insight "Ofertas laborales por empresa" muestra las empresas que publicaron las ofertas de trabajo extraídas de las plataformas Glassdoor,Linkedin y GetOnBoard. Puede filtrar por empresa y ver el nivel de presencia de dicha empresa en las distintas plataformas en la parte superior derecha, y debajo verá las ofertas laborales por región y empresa.""")
    
    cols = st.columns([1, 1, 1, 1, 1, 1])

    with cols[1]:
        components.html("""<iframe title="Pagina 1 - Herramientas" width="600" height="373.5" src="https://app.powerbi.com/view?r=eyJrIjoiOTY2NzliZjYtYTNjYS00Nzg5LWE4ODUtNjc0N2EzMzM0NTYyIiwidCI6IjNhNDIzMDA3LTk3MDktNGU0NS1iNGFmLTJmOTliZDM3ZDE4ZSJ9" frameborder="0" allowFullScreen="true"></iframe>""",1280,720)
    with st.expander("Ver explicación"):
        st.write("""El insight "Herramientas por perfil" muestra las herramientas tecnológicas más utilizadas por carrera. Sobre cada nombre de las herramientas puede ver el porcentaje que representan dentro del stack tecnológico y arriba puede ver el porcentaje de ellas que representan el 80% de la demanda por carrera.""")


def arhivo_pdf(archivo, kmeans):
    pdf = PdfReader(archivo)                # lee pdf
    numeropaginas = len(pdf.pages)          # cuenta las paginas

    text = ''
    for i in range(numeropaginas):          # utiliza un bucle para guardar todo el texto del PDF
        pagina = pdf.pages[i] 
        text = text + pagina.extract_text() 
    text = text.lower().replace("\n"," ").replace(":"," ").replace("\xa0"," ")


    fila = []                   # una vez con el texto completo, buscamos las herramientas en el documento y guardamos en "fila"
    for i in herramientas:
        if i in text:
            fila.append(1)      # 1 de haberlo encontrado
        else: fila.append(0)    # 0 en caso de que no lo encuentre


    text = np.asarray(fila).reshape(1,-1)       # transformamos para evaluar kmeans
    preds=kmeans.predict(text)                  # consultamos el valor
    if preds[0] == 0: preds = "Data Engineer"   # dependiendo el valor se asignará su respuesta
    if preds[0] == 1: preds = "Data Analyst"
    if preds[0] == 2: preds = "Data Science"

    return preds    #resultado

def importar_csv(csv, kmeans):
    df = pd.read_csv(csv, encoding='windows-1252')       # leemos el csv
    descripcion = df.columns[0] # indicamos la columna a usar
    df = df[[descripcion]]      
    df = df.dropna()     # eliminamos nulos

    prediccion = []
    for descrip in df[descripcion]:     # en este bucle buscamos entre cada fila las cohincidencias
        fila = []                       # guardamos en fila
        for i in herramientas:
            if i in descrip.lower():
                fila.append(1)  # 1 si encontró el valor
            else: fila.append(0)                    # 0 no lo encontró

        fila= np.asarray(fila).reshape(1,-1)    # transformar la fila para evaluar
        preds=kmeans.predict(fila)              # kmeans evalúa el resultado


        if preds[0] == 0: preds = "Data Engineer"   #indica el resultado dependiendo el valor
        if preds[0] == 1: preds = "Data Analyst"
        if preds[0] == 2: preds = "Data Science"
        
        prediccion.append(preds)     #lo guarda en el DataFrame con su respectiva fila
    df['kmeans'] = prediccion

    return df   #muestra el DataFrame terminado

MODEL_PATH = '../Machine Learning/clasificacion_areas_del_data.pkl'
herramientas = ['python','excel','tableau','jupyter','matplotlib','machine learning','engineer','engineering''tensorflow',
 'jupyter notebook','apache''power bi','sql','postgresql','mongodb','airflow','big query','hive','ingeniero software','science',
 'scientists','scientific','scientist','analytics','analysis','analyst','datos','data','cloud','frameworks','numpy','pandas',
 'etl','big data','hadoop','pipeline','dashboards', 'visualization', 'storytelling','deep learning','aws','azure']

with tab2:

    model=''

    # Se carga el modelo
    if model=='':
        with open(MODEL_PATH, 'rb') as file:
            model = pickle.load(file)

    st.write("Ingrese el archivo pdf")

    upload = st.file_uploader('Elegir Archivo pdf')
    if upload is not None:
        st.write(arhivo_pdf(upload,model))

    upload = st.file_uploader('Elegir Archivo csv')
    if upload is not None:
        st.write(importar_csv(upload,model))


st.markdown('***')