from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, INTEGER, VARCHAR, ForeignKey, FLOAT, DATETIME
#####################################################################
###          Scrip para la creacion del modelo relacional         ###
###                           en cloudsql                         ###
#####################################################################
db_username = 'prueba' #Usuario externo que hemos creado
db_password = 'grupo05elmejor' #Contraseña del usuario externo
db_ip = '34.151.201.62' #IP externa de la instancia
db_name = 'data_proyect'#Nombre de la database

s = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(db_username, db_password, db_ip, db_name)
#creamos la conexion
engine = create_engine(s)

metadata_obj = MetaData()
#instanciamos todas las tablas con los PK y FK
#Tabla continente
continente = Table(
    'Continente',
    metadata_obj,
    Column('Continente', VARCHAR(150), primary_key=True)
)

#Tabla pais
pais = Table(
    "PaisesConContinentes",
    metadata_obj,
    Column("Pais", VARCHAR(150)),
    Column("idPais", VARCHAR(150), primary_key=True),
    Column('MegaContinente', VARCHAR(150)),
    Column('Continente_Continente', VARCHAR(150), ForeignKey('Continente.Continente')),
    Column('Latitud',FLOAT),
    Column('Longitud',FLOAT)
)

#Tabla empresa
empresa = Table(
    'Empresas_has_PaisesConContinentes',
    metadata_obj,
    Column('Empresa', VARCHAR(150), primary_key=True),
    Column('idPais', VARCHAR(150), ForeignKey('PaisesConContinentes.idPais')),
    Column('Continente', VARCHAR(150), ForeignKey('Continente.Continente'))
)

#Tabla RatingEmpresas
ratingEmpresa = Table(
    'RatingsEmpresa',
    metadata_obj,
    Column('idReview', INTEGER, primary_key=True),
    Column('rateCO', INTEGER),
    Column('rateCyB', INTEGER),
    Column('rateCV', INTEGER),
    Column('rateGV', INTEGER),
    Column('rateOAM', INTEGER),
    Column('rateWLB', INTEGER),
    Column('rateCA', INTEGER),
    Column('ratePBV', INTEGER),
    Column('rateRTF', INTEGER),
    Column('Empresa', VARCHAR(150), ForeignKey('Empresas_has_PaisesConContinentes.Empresa')),
    Column('idPais', VARCHAR(150), ForeignKey('PaisesConContinentes.idPais')),
    Column('Continente', VARCHAR(150), ForeignKey('Continente.Continente'))
)

#Tabla plataformaEmpleo
plataformaEmpleo = Table(
    'PlataformasEmpleo',
    metadata_obj,
    Column('PlataformasEmpleo', VARCHAR(150), primary_key=True)
)

#Tabla modalidad
modalidad = Table(
    'Modalidad',
    metadata_obj,
    Column('Modalidad', VARCHAR(150), primary_key=True)
)

#Tabla puestoTrabajo
puestoTrabajo = Table(
    'PuestoTrabajo',
    metadata_obj,
    Column('PuestoTrabajo', VARCHAR(150), primary_key=True)
)


#Tabla Herramientas
herramientas = Table(
    'herramienta',
    metadata_obj,
    Column('herramienta', VARCHAR(150), primary_key=True)
)

#Tabla ofertasLaborales
ofertasLaborales = Table(
    'OfertasLaborales',
    metadata_obj,
    Column('idOfertasLaborales', INTEGER, primary_key=True),
    Column('salario', INTEGER),
    Column('Empresa', ForeignKey('Empresas_has_PaisesConContinentes.Empresa')),
    Column('Modalidad', ForeignKey('Modalidad.Modalidad')),
    Column('PlataformasEmpleo', ForeignKey('PlataformasEmpleo.PlataformasEmpleo')),
    Column('Fecha', DATETIME),
    Column('PuestoTrabajo',ForeignKey('PuestoTrabajo.PuestoTrabajo')),
    Column('pais',VARCHAR(150)),
    Column('idPais', VARCHAR(150), ForeignKey('PaisesConContinentes.idPais')),    
    Column('Continente',VARCHAR(150),ForeignKey('Continente.Continente'))
)

#Tabla herr_carrera
herr_carera = Table(
    'herr_carrera',
    metadata_obj,
    Column('herramienta', VARCHAR(150), ForeignKey('herramienta.herramienta')),
    Column('cuenta', INTEGER),
    Column('PuestoTrabajo', VARCHAR(150),ForeignKey('PuestoTrabajo.PuestoTrabajo'))
)

metadata_obj.create_all(engine)