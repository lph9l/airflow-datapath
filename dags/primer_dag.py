from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definición de argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Creación del DAG
dag = DAG(
    'example_dag',  # Identificador único del DAG
    default_args=default_args,
    description='Un DAG de ejemplo que imprime la fecha y un saludo',
    schedule_interval=timedelta(days=1),  # Se ejecuta diariamente
    start_date=datetime(2000, 1, 1),      # Fecha de inicio fija
    catchup=False,                        # No ejecuta runs pasados
)

# Tarea 1: imprime la fecha actual
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Tarea 2: espera 5 segundos
t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    dag=dag,
)

# Tarea 3: imprime un saludo
t3 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello Airflow!"',
    dag=dag,
)

# Definición de dependencias
t1 >> t2 >> t3
