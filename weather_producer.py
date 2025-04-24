import os
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv()
#api_key = os.getenv('OPENWEATHER_API_KEY')
api_key = '92ac1561e5d0eccf9f54619896ddc504'
producer = Producer({'bootstrap.servers': 'localhost:9092'})
def fetch_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    return response.json()

# Delivery of the messages in realtime

# Kafka Producer (Data Sender)
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    city = "Nairobi"
    weather_data = fetch_weather(city)
    producer.produce('weather_topic', value=str(weather_data), callback=delivery_report)
    producer.flush()

## NB: Confluent-kafka is a high-level and reliable Python client for Apache Kafka compatible with all brokers     