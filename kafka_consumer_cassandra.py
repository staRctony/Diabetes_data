from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from confluent_kafka import Consumer, KafkaException
import json

# Cassandra setup
auth_provider = PlainTextAuthProvider('clientId', 'secret')
cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
session = cluster.connect()

# Create a keyspace and table
session.execute("""
CREATE KEYSPACE IF NOT EXISTS diabetes_data 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
""")
session.set_keyspace('diabetes_data')

session.execute("""
CREATE TABLE IF NOT EXISTS diabetes (
    year INT,
    gender TEXT,
    age INT,
    location TEXT,
    race_africanamerican INT,
    race_asian INT,
    race_caucasian INT,
    race_hispanic INT,
    race_other INT,
    hypertension INT,
    heart_disease INT,
    smoking_history TEXT,
    bmi FLOAT,
    hba1c_level FLOAT,
    blood_glucose_level INT,
    diabetes TEXT,
    PRIMARY KEY (year, age)
);
""")

# Kafka Consumer setup
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'diabetes_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['diabetes_topic'])

# Helper function to safely convert to integer or float
def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return None  # Return None for invalid integer values

def safe_float(value):
    try:
        return float(value)
    except ValueError:
        return None  # Return None for invalid float values

# Handle missing age by returning a default value (-1) if it's invalid or missing
def safe_age(value):
    try:
        age = int(value)
        if age <= 0:
            return -1  # If the age is invalid or non-positive, set it to -1
        return age
    except ValueError:
        return -1  # Default to -1 if the age is not a valid integer

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        # Check if the message is empty
        if msg.value() is None or len(msg.value()) == 0:
            print("Received an empty message. Skipping.")
            continue

        try:
            # Decode the message and parse it as JSON
            row = json.loads(msg.value().decode('utf-8'))

            # Use safe_int, safe_float, and safe_age to handle conversion
            session.execute("""
            INSERT INTO diabetes (year, gender, age, location, race_africanamerican, race_asian,
            race_caucasian, race_hispanic, race_other, hypertension, heart_disease, smoking_history,
            bmi, hba1c_level, blood_glucose_level, diabetes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                safe_int(row['year']),
                row['gender'],
                safe_age(row['age']),  # Use safe_age here to handle invalid or missing age
                row['location'],
                safe_int(row['race:AfricanAmerican']),
                safe_int(row['race:Asian']),
                safe_int(row['race:Caucasian']),
                safe_int(row['race:Hispanic']),
                safe_int(row['race:Other']),
                safe_int(row['hypertension']),
                safe_int(row['heart_disease']),
                row['smoking_history'],
                safe_float(row['bmi']),
                safe_float(row['hbA1c_level']),
                safe_int(row['blood_glucose_level']),
                row['diabetes']
            ))

            print(f"Inserted: {row}")

        except json.JSONDecodeError:
            print(f"Malformed message received, skipping: {msg.value()}")

except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    consumer.close()
