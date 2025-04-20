from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'weather_etl',
    default_args=default_args,
    description='A simple weather ETL pipeline',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 4, 20),
    catchup=False,
) as dag:

    def extract():
        """Extract weather data from the API."""
        API_KEY = 'your_api_key_here'
        CITY = 'Nairobi'
        URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric'
        response = requests.get(URL)
        return response.json()

    def transform(data):
        """Transform the extracted data."""
        return {
            'city': data['name'],
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'weather': data['weather'][0]['description'],
            'timestamp': datetime.now()
        }

    def load(data):
        """Load the transformed data into PostgreSQL."""
        df = pd.DataFrame([data])
        engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/weather_db')
        df.to_sql('weather_data', engine, if_exists='append', index=False)

    # Define tasks
    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_args=['{{ task_instance.xcom_pull(task_ids="extract") }}'],
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load,
        op_args=['{{ task_instance.xcom_pull(task_ids="transform") }}'],
    )

    # Set task dependencies
    t1 >> t2 >> t3
