
import sqlalchemy as sa
from sqlalchemy import create_engine
import pandas as pd 
import requests
import redshift_connector
from sqlalchemy.engine.url import URL
import sqlalchemy_redshift
import traceback
from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

# argumentos por defecto para el DAG
default_args = {
    'owner': 'marianolopez_coderhouse',
    'start_date': datetime(2023,8,20),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

Dag_Nasdaq = DAG(
    dag_id='Nasdaq',
    default_args=default_args,
    description='Valores del Nasdaq',
    schedule_interval="@daily",
    catchup=False
)






def obtener_datos_desde_api(url):
    try:
        # Realizar una solicitud GET a la API
        response = requests.get(url)

        # Verificar el código de estado de la respuesta
        if response.status_code == 200:
           
           with open("temp.csv", "wb") as f:
                f.write(response.content)
                datos_file = "temp.csv"
                datos = pd.read_csv(datos_file)
                return datos
                
        else:
            print(f"Error al obtener los datos. Código de estado: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Error de conexión: {e}")
        return None


def transformar_data(datos_file):
        datos = pd.read_csv(datos_file)
        #datos = datos.drop_duplicates()
        #print(datos)
        return datos
 
# URL de la API que deseas consultar
#api_url = 'https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.csv'

# Llamar a la función para obtener los datos
#datos_file = obtener_datos_desde_api(api_url)

#if datos_file:
#    try:

def conectar_redshift(datos):
    try:  
        dag_path = os.getcwd()     #path original.. home en Docker

        with open(dag_path+'/keys/'+"user.txt",'r') as f:
            user= f.read()
        with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
            pwd= f.read()

        url = URL.create(
            drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com', # Amazon Redshift host
            port=5439, # Amazon Redshift port
            database='data-engineer-database', # Amazon Redshift database
            username=user, # Amazon Redshift username
            password=pwd # Amazon Redshift password
        )

        print('test 1')
        engine = sa.create_engine(url)
        conn = engine.connect()
        print('test 2')

        
        # Crear el motor de sqlalchemy para utilizar el método to_sql
        #print('test 4')
    
        

        # Nombre de la tabla en Redshift donde se cargarán los datos
        table_name = 'nasdaq_table'
        schema_name = 'marianolopez7749_coderhouse'

        # Cargar el DataFrame en la tabla de Redshift
        datos.to_sql(
            name=table_name,
            schema=schema_name,
            con=conn,
            if_exists='replace',
            index=False
        )

        print('test 5')
        
          
        engine.close()
        
        engine.dispose()
        print("Datos cargados exitosamente en Redshift.")

    except Exception as e:
        print(f"Error al cargar los datos en Redshift: {e}")

        traceback.print_exc()  # Print the full exception traceback for debugging purposes

    finally:
        if 'conn' in locals() and engine is not None:
            try:
                
                engine.close()
                
            except Exception as e:
                print(f"Error al cerrar la conexión: {e}")
                traceback.print_exc()

        if 'engine' in locals() and engine is not None:
           engine.dispose()
#else:
#     print("No se pudieron obtener los datos desde la API.")



task_1 = PythonOperator(
    task_id='obtener_datos_desde_api',
    python_callable=obtener_datos_desde_api,
    op_args=['https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.csv'],
    dag=Dag_Nasdaq,
)

#task_2 = PythonOperator(
#    task_id='transformar_data',
#    python_callable=transformar_data,
#    op_args=['{{datos_file}}'],
#    dag=Dag_Nasdaq,
#)

task_3 = PythonOperator(
    task_id='conectar_redshift',
    python_callable=conectar_redshift,
    op_args=['{{datos}}'],
    dag=Dag_Nasdaq,
)



task_1 >> task_3


