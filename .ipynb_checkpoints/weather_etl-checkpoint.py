"""""
Use python to create a simple ETL data pipeline to extract, transform and load weather 
from a REST API to save as a CSV file
"""
#import os
import requests
import pandas as pd
from datetime import datetime
import psycopg2  #DB Adapter for python - Connecting to PostgreSQL & executing queries
from sqlalchemy import create_engine #Efficiently manage & reuse db connections
#from dotenv import load_dotenv


##load sensitive data from .env
#load_dotenv()
#api_key = os.getenv("API_KEY")
API_KEY = '92ac1561e5d0eccf9f54619896ddc504'
CITY = 'Nairobi'

# PostgreSql db credentials
DB_USER = 'postgres'
DB_PASSWORD = 'admin1234'
DB_HOST = 'localhost'  # or your database host
DB_PORT = '5432'       # default PostgreSQL port
DB_NAME = 'weather_db'


# Define a base url for Open Weather  - fstring
URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric'

# Extract data
def extract():
    response = requests.get(URL)
    data = response.json()
    return data

## Transform data
def transform(data):
    transformed_data = {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'weather': data['weather'][0]['description'],
        'timestamp': datetime.now()
    }
    return transformed_data

## Load data
def load(data):
    df = pd.DataFrame([data])
   # df.to_csv('weather_data.csv', mode='a', header=not pd.io.common.file_exists('weather_data.csv'), index=False)
    #print("Data Loaded successfuly")
    # Establish a connection using SQLAlchemy Engine
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    # Load data into PostgreSQL
    table_name = 'weather_data'
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print('Data loaded successfully into PostgreSQL.')


    # Close the engine connection 
    engine.dispose()

def run_etl():
    raw_data = extract()
    processed_data = transform(raw_data)
    load(processed_data)

if __name__ == '__main__':
    run_etl()

