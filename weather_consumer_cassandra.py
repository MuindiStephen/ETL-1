from cassandra.cluster import Cluster
from confluent_kafka import Consumer, KafkaException
import json

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS weather WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
session.set_keyspace('weather')
session.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        city text,
        timestamp timestamp,
        temperature float,
        humidity int,
        PRIMARY KEY (city, timestamp)
    );
""")

# Set up Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weather_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['weather_topic'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # Decode and parse JSON
        msg_value = msg.value().decode('utf-8')
        print("⚠️ Raw Kafka Message:", msg_value)  # Optional: Debug print

        data = json.loads(msg_value)  # ✅ This works now if producer sends valid JSON

        # Extract fields
        city = data['name']
        timestamp = data['dt']
        temperature = data['main']['temp']
        humidity = data['main']['humidity']

        # Insert into Cassandra
        session.execute("""
            INSERT INTO weather_data (city, timestamp, temperature, humidity)
            VALUES (%s, toTimestamp(now()), %s, %s)
        """, (city, temperature, humidity))

except KeyboardInterrupt:
    print("🛑 Consumer stopped by user.")
finally:
    consumer.close()
