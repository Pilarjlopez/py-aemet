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
years=[*range(2003, 2013)]
estation="6325O" # ALMERIA/AEROPUERTO
autoconfirm=True
fetchmetadata=True

#######

def obtain_data_for_year(year, save_metadata=False, autoconfirm=False):
    try:
        print(f'Solcitando los datos a la API de AEMET para el año {year}...')
        start_date=f'{year}-01-01T00:00:00UTC'
        end_date=f'{year}-12-31T23:59:59UTC'
        url=f'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{estation}'
        print("La consulta es " + url)
        req_prepare_data = requests.request(method='GET', url=url, headers=headers, params={"api_key":api_key })
        json_prepare_data = json.loads(req_prepare_data.text)
        if json_prepare_data['estado'] != 200:
            print("Error al obtener los datos. HTTP Status:", json_prepare_data['estado'], json_prepare_data['descripcion'])
            quit()
        
        print("Recibido: ", json_prepare_data)

        print("Obteniendo los datos generados...")
        req_data = requests.request(method='GET', url=json_prepare_data['datos'], headers=headers)
        data = pd.DataFrame(req_data.json())
        if save_metadata:
            req_metadatadata = requests.request(method='GET', url=json_prepare_data['metadatos'], headers=headers)
            metadatadata = pd.DataFrame(req_metadatadata.json()['campos'])

        print()
        table = db_table_prefix + str(year)
        message = f'Todo listo para cargar los datos en la tabla "{table}" con los datos'
        if save_metadata:
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
            if save_metadata:
                metadatadata.to_sql(db_table_metadata, con=engine, if_exists='replace', index=False)
    except Exception as e:
        print("Error: ", e)


#######
for year in years:
    obtain_data_for_year(year, (fetchmetadata & (year == years[0])), autoconfirm)
