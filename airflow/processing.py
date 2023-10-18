import psycopg2
import requests

# Configuración de la API OpenWeatherMap
api_key = "01de7114b056082a373fa9dbee534292"  # Reemplaza con tu clave de API de OpenWeatherMap
base_url = "http://api.openweathermap.org/data/2.5/weather"

def get_weather_data(city_name, state_code, country_code):
    try:
        response = requests.get(base_url, params={"q": f"{city_name},{state_code},{country_code}", "appid": api_key})
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error en la conexión a la API OpenWeatherMap: {e}")
        return None

def save_to_database(city_name, state_code, country_code, data, **kwargs):
    conn = psycopg2.connect(
        host='localhost',
        port='5433',
        database='prueba',
        user='jefe',
        password='19931993'
    )

    try:
        cursor = conn.cursor()
        temperature = data['main']['temp']
        humidity = data['main']['humidity']
        weather_description = data['weather'][0]['description']
        raw_insert_query = """
        INSERT INTO raw_weather_data (city_name, state_code, country_code, raw_data)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(raw_insert_query, (city_name, state_code, country_code, str(data)))

        staging_insert_query = """
        INSERT INTO staging_weather_data (city_name, state_code, country_code, temperature, humidity, weather_description)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(staging_insert_query, (city_name, state_code, country_code, temperature, humidity, weather_description))

        weather_data_insert_query = """
        INSERT INTO weather_data (city_name, state_code, country_code, temperature, humidity, weather_description, date)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """
        cursor.execute(weather_data_insert_query, (city_name, state_code, country_code, temperature, humidity, weather_description))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error al insertar en la base de datos: {e}")