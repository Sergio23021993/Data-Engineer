import psycopg2
import requests

# Configuración de la API OpenWeatherMap
api_key = "01de7114b056082a373fa9dbee534292"  # Reemplaza con tu clave de API de OpenWeatherMap
base_url = "http://api.openweathermap.org/data/2.5/weather"
city_name = "Cordoba"
country_code = "Argentina"

# Verificar la conexión a la API
try:
    response = requests.get(base_url, params={"q": f"{city_name},{country_code}", "appid": api_key})
    response.raise_for_status()
    print("Conexión a la API OpenWeatherMap exitosa")
except requests.exceptions.RequestException as e:
    print(f"Error en la conexión a la API OpenWeatherMap: {e}")

# Configuración de la conexión a la base de datos PostgreSQL
db_config = {
    "host": "localhost",
    "port":"5433",
    "database": "prueba",
    "user": "jefe",
    "password": "19931993" # la pass es 123456 en airflow
}

# Verificar la conexión a la base de datos
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.close()
    conn.close()
    print("Conexión a la base de datos PostgreSQL exitosa")
except psycopg2.Error as e:
    print(f"Error en la conexión a la base de datos PostgreSQL: {e}")
import psycopg2
import requests

# Configuración de la API OpenWeatherMap
api_key = "01de7114b056082a373fa9dbee534292"  # Reemplaza con tu clave de API de OpenWeatherMap
base_url = "http://api.openweathermap.org/data/2.5/weather"
city_name = "Cordoba"
country_code = "Argentina"

# Verificar la conexión a la API
try:
    response = requests.get(base_url, params={"q": f"{city_name},{country_code}", "appid": api_key})
    response.raise_for_status()
    print("Conexión a la API OpenWeatherMap exitosa")
except requests.exceptions.RequestException as e:
    print(f"Error en la conexión a la API OpenWeatherMap: {e}")

# Configuración de la conexión a la base de datos PostgreSQL
db_config = {
    "host": "localhost",
    "port":"5433",
    "database": "prueba",
    "user": "jefe",
    "password": "19931993" # la pass es 123456 en airflow
}

# Verificar la conexión a la base de datos
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.close()
    conn.close()
    print("Conexión a la base de datos PostgreSQL exitosa")
except psycopg2.Error as e:
    print(f"Error en la conexión a la base de datos PostgreSQL: {e}")