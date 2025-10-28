from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from CotyData_IPN import *


local_tz = pendulum.timezone("America/Argentina/Buenos_Aires")

def get_date_range():
    from datetime import date, timedelta
    today = date.today()
    today_weekday = today.weekday()
    
    if today_weekday == 0:
        date_from = today - timedelta(days=3)
        date_to = today - timedelta(days=1)
    else:
        date_from = today - timedelta(days=1)
        date_to = date_from
    
    return date_from, date_to

# Configuración por defecto del DAG
default_args = {
    'owner': 'lchero',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 8, tzinfo=local_tz),
    'email': ['lcherocotymania@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Definir el DAG - CORREGIDO: schedule en lugar de schedule_interval
dag = DAG(
    'dag_subir_maestros',
    default_args=default_args,
    description='DAG para cargar maestros de IPN',
    schedule='10 9 * * 1-5', 
    catchup=False,
    tags=['Pruebas', 'IPN', 'ETL', 'Familias', 'Categorias', 'Marcas', 'Atributos', 'Proveedores', 'Clientes']
)

def load_families_airflow():
    try:
        itemFamiliesCall.createFamiliesLoad()
    except Exception as e:
        print(f"Error al cargar familias: {e}")
        raise 

def load_categories_airflow():
    try:
        itemCategoriesCall.createCategoriesLoad()
    except Exception as e:
        print(f"Error al cargar categorias: {e}")
        raise

def load_marks_airflow():
    try:
        itemMarksCall.createMarksLoad()
    except Exception as e:
        print(f"Error al cargar marcas: {e}")
        raise

def load_categories_atributes_airflow():
    try:
        categoriesAttributesCall.createCategoriesAttributesLoad()
    except Exception as e:
        print(f"Error al cargar atributos de categorias: {e}")
        raise

def load_attributes_airflow():
    try:
        attributesCall.createAttributesLoad()
    except Exception as e:
        print(f"Error al cargar atributos: {e}")
        raise

def load_suppliers_airflow():
    try:
        SupplierCall.createSupplierLoad()
    except Exception as e:
        print(f"Error al cargar proveedores: {e}")
        raise

def load_clients_airflow():
    try:
        date_from, date_to = get_date_range()
        CustomersCall.createCustomers(rz_list=[1,2,4],date_from=date_from, date_to=date_to)
    except Exception as e:
        print(f"Error al cargar clientes: {e}")
        raise

def load_items_airflow():
    try:
        date_from, date_to = get_date_range()
        itemCall.createItemsLoad(dateFrom=date_from, dateTo=date_to)
    except Exception as e:
        print(f"Error al cargar items: {e}")
        raise

def load_categories_oc_airflow():
    try:
        Cargar_Orden_Compra_categoria()
    except Exception as e:
        print(f"Error al cargar categorias de ordenes de compra: {e}")
        raise
# Tarea 1: Comando Bash
task1 = PythonOperator(
    task_id='load_families',
    python_callable=load_families_airflow,
    dag=dag,
)
task2 = PythonOperator(
    task_id='load_categories',
    python_callable=load_categories_airflow,
    dag=dag,
)
task3 = PythonOperator(
    task_id='load_marks',
    python_callable=load_marks_airflow,
    dag=dag,
)
task4 = PythonOperator(
    task_id='load_categories_attributes',
    python_callable=load_categories_atributes_airflow,
    dag=dag,
)
task5 = PythonOperator(
    task_id='load_attributes',
    python_callable=load_attributes_airflow,
    dag=dag,
)
task6 = PythonOperator(
    task_id='load_suppliers',
    python_callable=load_suppliers_airflow,
    dag=dag,
)
task7 = PythonOperator(
    task_id='load_clients',
    python_callable=load_clients_airflow,
    dag=dag,
)
task8 = PythonOperator(
    task_id='load_items',
    python_callable=load_items_airflow,
    dag=dag,
)
task9 = PythonOperator(
    task_id='load_categories_oc',
    python_callable=load_categories_oc_airflow,
    dag=dag,
)

# Definir dependencias
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8>> task9