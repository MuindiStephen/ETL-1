import pandas as pd
from sqlalchemy import create_engine

# Replace with your actual database credentials
engine = create_engine('postgresql+psycopg2://postgres:admin1234@localhost:5432/weather_db')
df = pd.read_sql('SELECT * FROM weather_data', engine)

# reading data from a csv
#df = pd.read_csv('weather_data.csv')

# Display the first few rows
print(df)