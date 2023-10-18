from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_processing import get_weather_data, save_to_database

# Define tus argumentos DAG
default_args = {
    'owner': 'tu_nombre',
    'start_date': datetime(2023, 10, 15),
    'retries': 1,
}

# Crea el objeto DAG
dag = DAG(
    'weather_data_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Puedes ajustar el intervalo de programación aquí
    catchup=False,
)

# Lista de ciudades para las que deseas obtener datos
ciudades = [
    {'city_name': 'Nueva York', 'state_code': 'NY', 'country_code': 'US'},
    {'city_name': 'París', 'country_code': 'FR'},
    {'city_name': 'Sídney', 'country_code': 'AU'},
    {'city_name': 'Londres', 'country_code': 'GB'}
]

# Define las tareas del DAG
for city in ciudades:
    # Tarea para obtener datos del clima
    t1 = PythonOperator(
        task_id=f'get_weather_data_task_{city["city_name"]}',
        python_callable=get_weather_data,
        op_args=[city["city_name"], city.get("state_code"), city["country_code"]],
        provide_context=True,
        dag=dag,
    )

    # Tarea para guardar datos en la base de datos
    t2 = PythonOperator(
        task_id=f'save_to_database_task_{city["city_name"]}',
        python_callable=save_to_database,
        op_args=[city["city_name"], city.get("state_code"), city["country_code"]],
        provide_context=True,
        dag=dag,
    )

    # Define las dependencias entre las tareas
    t1 >> t2