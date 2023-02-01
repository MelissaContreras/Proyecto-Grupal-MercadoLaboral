# <center>***DATA WORLD: TENDENCIAS DEL MERCADO LABORAL*** </center>

## Proyecto Grupal

Integrantes: Melissa Contreras, Carolina Sosa, Sebastian Ezequiel, Luz Estrada y Facundo Lopez.

![](https://www.infoposiciones.net/wp-content/uploads/mercado-trabajo-Large-1.jpg)


**Contexto:** 
 
El mercado laboral tecnológico a nivel mundial está experimentando un gran crecimiento debido al aumento de la demanda de habilidades en áreas como la inteligencia artificial, la ciberseguridad y el desarrollo de aplicaciones. Según el informe "Tendencias del empleo en tecnología" de Indeed, las búsquedas de empleo en tecnología aumentaron un 34% en todo el mundo en 2020.

La demanda en el sector de Big Data ha experimentado un crecimiento significativo en los últimos años debido al aumento de la cantidad de datos generados y al aumento de la necesidad de analizarlos para obtener información valiosa. Según un informe de IDC (International Data Corporation), el gasto mundial en tecnologías de Big Data y analítica alcanzó los 274.3 mil millones de dólares en 2022, con una tasa de crecimiento compuesto anual del 11,9% desde 2018.

A nivel de Latinoamérica el crecimiento del mercado laboral tecnológico también ha sido significativo en los últimos años, con una tasa de crecimiento del 7% anual. Específicamente en el sector de Big Data y analítica está experimentando una tasa de crecimiento del 20% anual en la región. Según un informe de la consultora de recursos humanos Michael Page.

En países como Brasil, México y Colombia, el sector tecnológico está impulsando el crecimiento económico y la creación de empleos. Además, países como Argentina y Chile cuentan con un gran talento en tecnología y están atrayendo a empresas tecnológicas extranjeras para establecerse allí.

La demanda de habilidades en Big Data está aumentando en todo el mundo, especialmente en áreas como el aprendizaje automático, la inteligencia artificial y la visualización de datos. Los profesionales con habilidades en Big Data, incluyendo científicos de datos, ingenieros de datos y analistas de datos, son altamente valorados en el mercado laboral y se espera que la demanda continúe aumentando en el futuro.


 ---
 **Problemática:**

 El campo laboral y las tendencias respecto a los perfiles y tecnologías relacionadas a big data no son muy claras debido a su acelerado crecimiento. Una empresa reclutadora tiene el gran reto de poder integrar las demandas de empresas con la oferta de profesionales que existe en el mercado. Dentro de los retos que tiene que afrontar es que muchas empresas al momento de compartir una oferta laboral tienen problemas para identificar los perfiles que serían los más adecuados a sus necesidades.



#### *Ejemplo:*
#### - Aquí observamos que a para un puesto de Ingeniero de datos, piden    saber herramientas de visualización propias del perfil de Data Analytics. 


<p align="center"> <img src = 'source\ejemplo-anuncio.png'  /> </p>

 

 
 ---
  **Solución:**

   La solución propuesta por el equipo que conformamos es brindar una herramienta para la empresa recruiter en la que pueda apoyarse para tomar mejores decisiones en base a datos(distintas métricas y KPIs) observando las tendencias del mercado laboral a nivel mundial y regional, es decir, realizar una comparativa entre ambas.

   Esta herramienta de la que hablo es un sitio web, en el cual se observará 2 áreas:
   - La primera: Los insights (trabajos más demandados, herramientas más pedidas, variaciones de sueldo por puesto o a través del tiempo,etc)

   - La segunda: Un filtro en el que el recruiter podrá saber a que perfil pertenece el postulante en base a las herramientas tecnológicas que maneja.
   Este filtro será posible gracias a un modelo de machine learning de clasificación no supervisado.




 ---

## Nuestro Camino

La metodología Scrum es un proceso para llevar a cabo un conjunto de tareas de forma regular con el objetivo principal de trabajar de manera colaborativa, es decir, para fomentar el trabajo en equipo.

Con este método de trabajo lo que se pretende es alcanzar el mejor resultado de un proyecto determinado. Las prácticas que se aplican con la metodología Scrum se retroalimentan unas con otras y la integración de las mismas tiene su origen en un estudio de cómo hay que coordinar a los equipos para ser potencialmente competitivos.

En Scrum se van realizando entregas regulares y parciales del trabajo final, de manera prioritaria y en función del beneficio que aportan dichas entregas a los receptores del proyecto. Por este motivo, es una metodología especialmente indicada para proyectos complejos, con requisitos cambiantes y en los que la innovación y la flexibilidad son protagonistas.



--->--->--->--->--->--->--->--->--->--->

## **Pipeline**

<p align="center"> <img src = 'source\pipeline.png' /> </p>
 ---

## Nuestro Hallazgos
<p>En el análisis de los datos obtenidos, se trató de identificar las necesidades de información de la reclutadora. Teniendo en cuenta que el objetivo es estar al pendiente de las tendencias del mercado en torno a los 3 perfiles laborales definidos al inicio de este trabajo (data analyst, data engineer y data scientist).
 Se construyeron métricas que van alineados a tres objetivos específicos: 
 1) Conocer el estado de los sueldos y tendencias en el mercado
 2) Identificar las ofertas laborales a nivel mundial y su comparación con Sudamérica
 3) Identificar los requerimientos de las herramientas por perfil que actualmente el mercado está definiendo.<p> 

 ### **- Estado del mercado de sueldos**
 
<p>En la siguiente imagen se muestra cómo es el estado del mercado de sueldos. El KPI definido fue "La variación de los sueldos de forma trimestral por año" en las cuales se observa si ha habido un aumento o disminución de los sueldos en las ofertas laborales. Está clasificado por los tres perfiles para tener una mejor idea comparativa entre ellos. Asimismo, se ha generado un segmentador temporal por año y trimestre, y otro segmentador geográfico por región (estas regiones están definidas como los continentes del mundo). En la parte izquierda inferior se observa un mapa que muestra cómo es el sueldo promedio ubicado geográficamente. En la parte derecha inferior se muestra un gráfico de líneas de la variación de los sueldos a lo largo de finales del 2018 - 2020, lo cual permite identificar la tendencia de los sueldos a lo largo de los años.<p>
 
<p> Estas métricas nos muestran insights muy interesantes. El mapa de distribución de sueldos muestra según el tamaño de las burbujas que Europa destaca por tener ofertas salariales más elevadas, y Asia, por tener los de menor valor. Sudamérica muestra un valor promedio. Por otro lado, el gráfico de líneas muestra las tendencias en los sueldos a lo largo del tiempo. Se observa una diferencia notoria entre los analyst, enginer y scientist hasta antes del marzo 2019, destacandose los engineer con los ingresos más altos. Sin embargo, después de marzo 2019 esta diferencia salarial ya no es tan notoria, y es más, se ha llegado a un punto de equilibrio entre los tres para un valor de salario 74' 000 USD anuales. <p> 
 

 <p align="center"> <img src = 'source\Estado mercado sueldos.png' ><p>
  
 ### **- Estado del mercado de ofertas laborales**
  
<p> Otra de las métricas de interés identificadas es el estado de las ofertas laborales por empresa, y puestos de trabajo. Eso se muestra en la siguiente imagen, en la que se muestran las empresas que han compartido mayores ofertas laborales alineadas a los perfiles definidos. Asimismo, en el gráfico horizontal de barras del lado derecho, se muestra la demanda de puestos de trabajo por perfil, y por continente.
  
  Estos gráficos también nos muestran insights interesantes. se observa que la empresa Citi, Amazon, Hays, y Accenture destacan como las empresas con mayores ofertas laborales publicadas en las plataformas de empleo analizadas (LinkedIn, Glassdoor, GetOnBoard). Estas son potenciales usuarios principales de los servicios de la reclutadora, por lo que es importante tenerlos bien identificados. Asimismo, identificar los perfiles más requeridos por empresa es de mucho interés. Se observa que los data analyst tienen mayor demanda, seguidos por la scientists, y finalmente los engineers. Latinoamérica, comparado con otros países, muestra la misma proporción de demanda de analyst, scientist e engineer, pero, la cantidad es mucho menor comparada con Europa y Estados Unidos. <p>
  
  
 <p align="center"> <img src = 'source\Estado mercado ofertas laborale.png' ><p>
  
   ### **- Requerimientos de herramientas por perfil**
  
<p>  Analizar la demanda de herramientas por perfil. Esta fue una de las principales necesidades de información requeridas, pues una reclutadora tiene la difícil tarea de alinear los requerimientos de una empresa con el perfil de un postulante. Una de las formas de realizar esa alineación es a través del conocimiento del requerimiento del mercado por perfil. Uno de los KPI's construidos es la diversidad de herramientas que piden las empresas. Este se contruyó identificando cuáles fueron las herramientas que representaron el 80% de la demanda, y se dividió entre el total de herramientas identificadas por perfil. Este KPI nos mostró que los perfiles de data science tienen que tener un portafolio más diverso de herramientas con un 28% del total, entre los que se incluye: python, r, sql, tensorflow, sas, excel, tableau; mientras que los data analyst requieren una menor diversidad de herramientas con un 16 %, destacándose: sql, excel, python y tableau. Asimismo, se observa que dentro de las herramientas que se piden para un data scientist esta tableau, y excel, y estas son herramientas que están mejor alineadas para un data anlyst, demostrándose una necesidad para definir de forma más adecuada los requerimientos en las ofertas laborales. <p>
  
  
 <p align="center"> <img src = 'source\Requerimientos herramientas por perfil.png' ><p>


 ## Modelo Machine learning
 <p> Como mencionamos anteriormente, parte de nuestra solución es brindar una correcta descripción sobre las tecnologías que utiliza un Cientifico de Datos. Para esto, durante el estudio de nuestra carrera nos encontramos capacitados para utilizar ciertas herramientas que puedan mejorar procesos y agilizar su funcionamiento.
  Estas herramientras nos describen en parte a nuestras ramas de la cienca de datos, así como un Data Analyst conoce sobre visualización y representación de los datos, también conoce correctamente el uso de Microsoft Excel, Power BI, Tableau, etc. entre otras. Así mismo para el área del Data Engineer el uso de SQL por ejemplo y también Data Science utilizando ciertas librerías en Python como el algoritmo k-Nearest Neighbor para un modelo de entrenamiento clasificado. 
  Dicho esto, se utilizó un modelo de Clasificación sin supervisión K-Means Clustering para clasificar las ramas de la cienca de datos. El modelo está entrenado de acuerdo a los stacks tecnológicos, herramientas solicitadas y en las necesidades de puestos laborales.
  
  ¿Porqué K-Means Clustering?
   Hay diferentes modelos de clasificación no supervisada pero para nuestro objetivo, necesitamos agrupar de manera eficiente las tecnologías. Para esto, se eligió K-Means Clustering ya que es uno de los algoritmos de agrupamiento más populares que divide los datos en "K grupos" basados en la similitud de sus características.
  
Los invito a ingresar al archivo [Modelo de clasificacion final.ipynb](https://github.com/mayteet/ProyectoGrupal14MercadoLaboral/blob/main/Modelo%20de%20clasificacion%20final.ipynb) para ver en detalle la creación del modelo.
  
  
  <p align="center"> <img src = 'source\kmeans.png' ><p>
<p>
 

  ## Link de deployment
https://henry-test-streamlit-app-zhi7kb23aq-ue.a.run.app/
 
