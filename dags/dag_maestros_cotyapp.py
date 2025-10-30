from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
from CotyData_IPN import *
from utils import get_date_range

# Configuración por defecto del DAG
default_args = {
    'owner': 'lchero',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 8),
    'email': ['lcherocotymania@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'dag_subir_maestros_cotyapp',
    default_args=default_args,
    description='DAG para cargar maestros de IPN a CotyApp',
    schedule=None, 
    catchup=False,
    tags=['IPN', 'Proveedores', 'Articulos', 'CotyApp']
)

def load_suppliers_cotyapp_airflow():
    try:
        SupplierCallCotyApp.createSupplierLoad()
    except Exception as e:
        print(f"Error al cargar proveedores a CotyApp: {e}")
        raise

def load_items_cotyapp_airflow():
    try:
        date_from, date_to = get_date_range()
        ItemCallCotyApp.createItemsLoad(dateFrom=date_from, dateTo=date_to)
    except Exception as e:
        print(f"Error al cargar articulos a CotyApp: {e}")
        raise

task_load_suppliers_cotyapp = PythonOperator(
    task_id='load_suppliers_cotyapp',
    python_callable=load_suppliers_cotyapp_airflow,
    dag=dag,
)

task_load_items_cotyapp = PythonOperator(
    task_id='load_items_cotyapp',
    python_callable=load_items_cotyapp_airflow,
    dag=dag,
)

task_load_suppliers_cotyapp >> task_load_items_cotyapp