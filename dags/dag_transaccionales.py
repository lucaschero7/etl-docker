from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
}

dag = DAG(
    'dag_subir_transaccionales',
    default_args=default_args,
    description='DAG para subir datos transaccionales de IPN a la base de datos',
    schedule=None,  
    catchup=False,
    tags=['Transaccionales', 'IPN', 'Precios', 'Costos', 'Remito', 'Ventas', 'Compras', 'Recepciones', 'Recepciones_Mercaderia'],
)

def load_prices_airflow():
    try:
        date_from, date_to = get_date_range()
        PriceListCall.createPriceListLoad(date_from, date_to)
    except Exception as e:
        print(f"Error al cargar precios: {e}")
        raise

def load_costs_airflow():
    try:
        date_from, date_to = get_date_range()
        CostListCall.createCostListLoad(date_from, date_to)
    except Exception as e:
        print(f"Error al cargar costos: {e}")
        raise

def load_sales_airflow():
    try:
        date_from, date_to = get_date_range()
        salesDocumentsCall.createSalesDocumentsLoad(dateFrom=date_from, dateTo= date_to)
    except Exception as e:
        print(f"Error al cargar ventas: {e}")
        raise

def load_delivery_notes_airflow():
    try:
        date_from, date_to = get_date_range()
        DeliveryNotesCall.createDeliveryNotesLoad(date_from, date_to)
    except Exception as e:
        print(f"Error al cargar notas de entrega: {e}")
        raise

def load_purchase_orders_airflow():
    try:
        date_from, date_to = get_date_range()
        PurchaseOrderCall.createPurchaseOrderLoad(date_from, date_to)
    except Exception as e:
        print(f"Error al cargar órdenes de venta: {e}")
        raise

def load_goods_receipt_airflow():
    try:
        date_from, date_to = get_date_range()
        GoodsReceiptCall.createGoodsReceipt(date_from, date_to)
    except Exception as e:
        print(f"Error al cargar recibos de mercancías: {e}")
        raise
    
def load_receipts_airflow():
    try:
        Cargar_Recepciones_Mercaderia()
    except Exception as e:
        print(f"Error al cargar recibos: {e}")
        raise

def load_sales_orders_airflow():
    try:
        date_from, date_to = get_date_range()
        SalesOrder.createSalesOrder(date_from=date_from,date_to=date_to)
    except Exception as e:
        print(f"Error al cargar órdenes de venta: {e}")
        raise


task1 = PythonOperator(
    task_id='load_prices',
    python_callable=load_prices_airflow,
    dag=dag,
)

task2 = PythonOperator(
    task_id='load_costs',
    python_callable=load_costs_airflow,
    dag=dag,
)

task3 = PythonOperator(
    task_id='load_sales',
    python_callable=load_sales_airflow,
    dag=dag,
)

task5 = PythonOperator(
    task_id='load_delivery_notes',
    python_callable=load_delivery_notes_airflow,
    dag=dag,
)

task6 = PythonOperator(
    task_id='load_purchase_orders',
    python_callable=load_purchase_orders_airflow,
    dag=dag,
)

task7 = PythonOperator(
    task_id='load_goods_receipt',
    python_callable=load_goods_receipt_airflow,
    dag=dag,
)

task9 = PythonOperator(
    task_id='load_receipts',
    python_callable=load_receipts_airflow,
    dag=dag,
)

task10 = PythonOperator(
    task_id='load_sales_orders',
    python_callable=load_sales_orders_airflow,
    dag=dag,
)

trigger_dag_maestros_cotyapp = TriggerDagRunOperator(
    task_id='trigger_dag_maestros_cotyapp',
    trigger_dag_id='dag_subir_maestros_cotyapp',
    dag=dag,
)


# Definir dependencias
task1 >> task2 >> task3 >> task5 >> task6 >> task7 >> task9 >> task10 >> trigger_dag_maestros_cotyapp