from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Funciones de las tareas
def tarea1():
    print("Tarea 1 ejecutada")

def tarea2():
    print("Tarea 2 ejecutada")

def tarea3():
    print("Tarea 3 ejecutada")

# Configuraciones del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_ejemplo_secuencial_paralelo',
    default_args=default_args,
    description='Ejemplo DAG con tareas secuenciales y paralelas',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Tareas EmptyOperator para inicio y fin
inicio = EmptyOperator(task_id='inicio', dag=dag)
fin = EmptyOperator(task_id='fin', dag=dag)

# Tareas PythonOperator
tarea_1 = PythonOperator(task_id='tarea_1', python_callable=tarea1, dag=dag)
tarea_2 = PythonOperator(task_id='tarea_2', python_callable=tarea2, dag=dag)
tarea_3 = PythonOperator(task_id='tarea_3', python_callable=tarea3, dag=dag)

# Definir dependencias
inicio >> tarea_1  # Ejecuta tarea_1 después de inicio

# Ejecución en paralelo
tarea_1 >> [tarea_2, tarea_3]  # tarea_2 y tarea_3 se ejecutan en paralelo

# Finalizar después de que ambas tareas paralelas terminen
[tarea_2, tarea_3] >> fin
