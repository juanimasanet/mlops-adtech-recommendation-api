import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import boto3
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import numpy as np


def filtrar_datos():
    try:
        # Inicializar el cliente de S3
        s3 = boto3.client('s3')
        bucket_name = 'grupo-3-mlops'

        # Obtener la fecha actual
        today = datetime.now().date()

        # Leer los archivos de S3
        advertisers_df = pd.read_csv(s3.get_object(Bucket=bucket_name, Key='advertiser_ids.csv')['Body'])
        ads_views_df = pd.read_csv(s3.get_object(Bucket=bucket_name, Key='ads_views.csv')['Body'])
        product_views_df = pd.read_csv(s3.get_object(Bucket=bucket_name, Key='product_views.csv')['Body'])

        # Filtrar los datos
        active_advertisers = advertisers_df['advertiser_id'].unique()

        filtered_ads_views = ads_views_df[
            (ads_views_df['advertiser_id'].isin(active_advertisers)) &
            (pd.to_datetime(ads_views_df['date']).dt.date == today)
        ]

        filtered_product_views = product_views_df[
            (product_views_df['advertiser_id'].isin(active_advertisers)) &
            (pd.to_datetime(product_views_df['date']).dt.date == today)
        ]

        # Retornar datos como diccionarios para XCom
        return {
            'filtered_ads_views': filtered_ads_views.to_dict(),
            'filtered_product_views': filtered_product_views.to_dict()
        }

    except Exception as e:
        print(f"Error al filtrar los datos: {e}")
        return {'filtered_ads_views': {}, 'filtered_product_views': {}}
        
def calcular_top_ctr(ti, **kwargs):
    try:
        # Obtener datos desde XCom
        xcom_data = ti.xcom_pull(task_ids='filtrar_datos')
        filtered_ads_views_dict = xcom_data.get('filtered_ads_views', {})

        if not filtered_ads_views_dict:
            print("No hay datos para calcular el top CTR.")
            return {}

        # Convertir a DataFrame
        filtered_ads_views = pd.DataFrame.from_dict(filtered_ads_views_dict)

        # Filtrar clicks e impresiones
        clicks_df = filtered_ads_views[filtered_ads_views['type'] == 'click']
        impressions_df = filtered_ads_views[filtered_ads_views['type'] == 'impression']

        # Agrupar por advertiser_id y product_id
        clicks_count = clicks_df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')
        impressions_count = impressions_df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')

        # Combinar clicks e impresiones
        ctr_df = pd.merge(clicks_count, impressions_count, on=['advertiser_id', 'product_id'], how='outer').fillna(0)

        # Calcular CTR evitando división por cero
        ctr_df['ctr'] = np.where(
            ctr_df['impressions'] > 0,
            ctr_df['clicks'] / ctr_df['impressions'],
            0
        )

        # Ordenar por CTR dentro de cada advertiser_id y tomar los top 20
        top_ctr_df = ctr_df.sort_values(['advertiser_id', 'ctr'], ascending=[True, False])
        top_ctr_df = top_ctr_df.groupby('advertiser_id').head(20).reset_index(drop=True)

        # Convertir a diccionario para devolver
        return top_ctr_df.to_dict(orient='records')

    except Exception as e:
        print(f"Error al calcular el top CTR: {e}")
        return {}

def calcular_top_product(ti, **kwargs):
    try:
        # Obtener datos desde XCom
        xcom_data = ti.xcom_pull(task_ids='filtrar_datos')
        filtered_product_views_dict = xcom_data.get('filtered_product_views', {})

        if not filtered_product_views_dict:
            print("No hay datos para calcular el top de productos.")
            return {}

        # Convertir a DataFrame
        filtered_product_views = pd.DataFrame.from_dict(filtered_product_views_dict)

        # Agrupar por advertiser_id y product_id
        top_product_data = filtered_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')

        # Ordenar por views dentro de cada advertiser_id y tomar los top 20
        top_products_df = top_product_data.sort_values(['advertiser_id', 'views'], ascending=[True, False])
        top_products_df = top_products_df.groupby('advertiser_id').head(20).reset_index(drop=True)

        # Convertir a diccionario para devolver
        return top_products_df.to_dict(orient='records')

    except Exception as e:
        print(f"Error al calcular el top de productos: {e}")
        return {}

def escribir_en_bd(**kwargs):
    print("Inicio de la tarea: escribir_en_bd")
    db_config = {
        'dbname': 'grupo_3',
        'user': 'postgres',
        'password': 'GRUPO3RDS2024',
        'host': 'grupo-3-25-11.cf4i6e6cwv74.us-east-1.rds.amazonaws.com',
        'port': 5432,
    }

    try:
        # Conexión a la base de datos
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Crear tablas si no existen
        create_table_queries = {
            "top_ctr_df": """
                CREATE TABLE IF NOT EXISTS top_ctr_df (
                    advertiser_id VARCHAR(50),
                    product_id VARCHAR(50),
                    impressions INT,
                    clicks INT,
                    ctr FLOAT
                );
            """,
            "top_products_df": """
                CREATE TABLE IF NOT EXISTS top_products_df (
                    advertiser_id VARCHAR(50),
                    product_id VARCHAR(50),
                    views INT
                );
            """
        }
        for query in create_table_queries.values():
            cur.execute(query)

        # Recuperar datos desde XCom
        ti = kwargs['ti']
        top_ctr_df_dict = ti.xcom_pull(task_ids='calcular_top_ctr')
        top_products_df_dict = ti.xcom_pull(task_ids='calcular_top_product')

        if not top_ctr_df_dict or not top_products_df_dict:
            print("No hay datos para escribir en la base de datos.")
            conn.close()
            return

        top_ctr_df = pd.DataFrame.from_dict(top_ctr_df_dict)
        top_products_df = pd.DataFrame.from_dict(top_products_df_dict)

        # Insertar datos en las tablas
        for _, row in top_ctr_df.iterrows():
            cur.execute(
            "INSERT INTO top_ctr_df (advertiser_id, product_id, impressions, clicks, ctr) VALUES (%s, %s, %s, %s, %s)",
            (row['advertiser_id'], row['product_id'], row['impressions'], row['clicks'], row['ctr'])
         )

        conn.commit()
        cur.close()
        conn.close()
        print("Datos escritos correctamente en PostgreSQL.")

    except Exception as e:
        print(f"Error al conectar o escribir en la base de datos: {e}")


# Definición del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='grupo3',
    default_args=default_args,
    description='Top CTR y top products por advertiser',
    schedule=None,
    start_date=datetime(2024, 11, 28),
    catchup=False,
) as dag:

    tarea_filtrar = PythonOperator(
        task_id='filtrar_datos',
        python_callable=filtrar_datos,
        provide_context=True,
    )

    tarea_top_ctr = PythonOperator(
        task_id='calcular_top_ctr',
        python_callable=calcular_top_ctr,
        provide_context=True,
    )

    tarea_top_product = PythonOperator(
        task_id='calcular_top_product',
        python_callable=calcular_top_product,
        provide_context=True,
    )

    tarea_escribir_bd = PythonOperator(
        task_id='escribir_en_bd',
        python_callable=escribir_en_bd,
        provide_context=True,
    )
    
    tarea_filtrar >> [tarea_top_ctr, tarea_top_product] >> tarea_escribir_bd
