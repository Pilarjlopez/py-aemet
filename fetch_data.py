import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

## Parámetros generales
api_key = os.getenv('AEMET_API_KEY')
headers = { 'cache-control': "no-cache" }

## Parámetros de base de datos
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_table_prefix = 'data'
db_table_metadata = 'metadata'

## Consulta
#years=[*range(1965, 1970)]
#years=[*range(1970, 1975)]
#years=[*range(1975, 1980)]
#years=[*range(1980, 1985)]
#years=[*range(1985, 1990)]
#years=[*range(1990, 1995)]
#years=[*range(1995, 2000)]
#years=[*range(2000, 2005)]
#years=[*range(2005, 2010)]
#years=[*range(2010, 2015)]
years=[*range(2015, 2020)]
#years=[*range(2020, 2024)]
estation="3195" # MADRID / Retiro
autoconfirm=True
fetchmetadata=True

#######

def obtain_data_for_year(year):
    try:
        # Itera sobre los meses del año en intervalos de 6 meses
        for month in range(1, 12, 6):
            # Obtiene los datos para el año y un intervalo de 6 meses
            data_aux = obtain_data_for_year_months(year=year,
                                               month_start="%02d" % (month,),
                                               month_end="%02d" % (month+5,))
            # Imprime la cantidad de registros obtenidos en el intervalo actual
            print(f'registros obtenidos {len(data_aux.index)}')
            # Si es el primer mes (enero), inicializa la variable 'data' con 'data_aux'
            if month == 1:
                data = data_aux
            else:
                # Si no es el primer mes, concatena 'data_aux' con 'data' existente
                data = pd.concat([data, data_aux], axis=0)
                 # Imprime la cantidad total de registros acumulados hasta ahora
            print(f'registros totales {len(data.index)}')
            # Retorna el DataFrame 'data' que contiene todos los registros del año
        return data
    # Si ocurre un error durante el proceso, lo imprime
    except Exception as e:
        print("Error: ", e)

def obtain_data_for_year_months(year, month_start, month_end):
    try:
        # Imprime un mensaje indicando que se están solicitando datos de la API de AEMET para el año y los meses especificados
        print(f'Solcitando los datos a la API de AEMET para el año {year} y los meses {month_start} and {month_end}...')
         # Define la fecha de inicio con el formato 'YYYY-MM-01T00:00:00UTC' usando el año y el mes de inicio proporcionados
        start_date=f'{year}-{month_start}-01T00:00:00UTC'
        # Define la fecha de fin con el formato 'YYYY-MM-31T23:59:59UTC' usando el año y el mes de fin proporcionados
        end_date=f'{year}-{month_end}-31T23:59:59UTC'
         # Construye la URL para la solicitud a la API de AEMET utilizando las fechas de inicio y fin, y el ID de la estación
        url=f'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{estation}'
        print("La consulta es " + url)
         # Realiza una solicitud GET a la API de AEMET para preparar la obtención de datos
        req_prepare_data = requests.request(method='GET', url=url, headers=headers, params={"api_key":api_key })
        # Convierte la respuesta de la solicitud a un objeto JSON
        json_prepare_data = json.loads(req_prepare_data.text)
        # Verifica si el estado de la respuesta es diferente de 200 (OK). Si es así, imprime un mensaje de error y termina la ejecución
        if json_prepare_data['estado'] != 200:
            print("Error al obtener los datos. HTTP Status:", json_prepare_data['estado'], json_prepare_data['descripcion'])
            quit()
            
        print("Recibido: ", json_prepare_data)

        print("Obteniendo los datos generados...")
        req_data = requests.request(method='GET', url=json_prepare_data['datos'], headers=headers)
         # Convierte la respuesta de la solicitud a un DataFrame de pandas
        data = pd.DataFrame(req_data.json())
        return data
    except Exception as e:
        print("Error: ", e)

def obtain_metadata():
    try:
        print(f'Obteniendo metadatos...')
        # Define la URL para la solicitud a la API de AEMET utilizando una fecha de inicio y fin arbitraria
        url=f'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/2000-01-01T00:00:00UTC/fechafin/2000-01-02T00:00:00UTC/estacion/{estation}'
        # Realiza una solicitud GET a la API de AEMET para preparar la obtención de datos
        req_prepare_data = requests.request(method='GET', url=url, headers=headers, params={"api_key":api_key })
        # Convierte la respuesta de la solicitud a un objeto JSON
        json_prepare_data = json.loads(req_prepare_data.text)
        req_metadatadata = requests.request(method='GET', url=json_prepare_data['metadatos'], headers=headers)
        metadata = pd.DataFrame(req_metadatadata.json()['campos'])
        return metadata
    except Exception as e:
        print("Error: ", e)   

def persist_data(year, data, metadata, autoconfirm=False):
    try:
        table = db_table_prefix + str(year)
        message = f'Todo listo para cargar los datos en la tabla "{table}" con los datos'
        if metadata is not None:
            message += f' y los metadatos en la tabla "{db_table_metadata}"'
        print(message)
        print('Atención, las tablas se van a borrar y a generar de nuevo con los datos obtenidos')
        print('')
        answer = ""
        if autoconfirm == True:
            answer = "s"
        while answer not in ["s", "n"]:
            answer = input("¿Seguro que deseas continuar? [S/N] ").lower()

        if answer == 's':
            engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
            data.to_sql(table, con=engine, if_exists='replace', index=False)
            if metadata is not None:
                metadata.to_sql(db_table_metadata, con=engine, if_exists='replace', index=False)
    except Exception as e:
        print("Error: ", e)

#######
for year in years:
    metadata = ((None, obtain_metadata()) [year == years[0]])
    data = obtain_data_for_year(year)
    persist_data(year=year, data=data, metadata=metadata, autoconfirm=autoconfirm)
