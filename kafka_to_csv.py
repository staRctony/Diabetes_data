import csv
from confluent_kafka import Consumer, KafkaException
import json

# Kafka Consumer setup
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'diabetes_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['diabetes_topic'])

# Helper functions
def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return None

def safe_float(value):
    try:
        return float(value)
    except ValueError:
        return None

def safe_age(value):
    try:
        age = int(value)
        return age if age > 0 else -1
    except ValueError:
        return -1

# CSV file setup
csv_file = "received_diabetes_data.csv"
batch_size = 1000  # Number of records per batch
batch = []

# Write headers to the CSV file
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([
        "year", "gender", "age", "location", "race_africanamerican", "race_asian", 
        "race_caucasian", "race_hispanic", "race_other", "hypertension", "heart_disease", 
        "smoking_history", "bmi", "hba1c_level", "blood_glucose_level", "diabetes"
    ])

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

        if msg.value() is None or len(msg.value()) == 0:
            print("Received an empty message. Skipping.")
            continue

        try:
            # Decode message
            row = json.loads(msg.value().decode('utf-8'))

            # Prepare record
            record = (
                safe_int(row['year']),
                row['gender'],
                safe_age(row['age']),
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
            )

            # Add record to batch
            batch.append(record)

            # Write to CSV in batches
            if len(batch) >= batch_size:
                with open(csv_file, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerows(batch)
                batch.clear()  # Clear the batch after writing to CSV

            print(f"Processed and saved: {record}")

        except json.JSONDecodeError:
            print(f"Malformed message received, skipping: {msg.value()}")

except KeyboardInterrupt:
    print("Stopping consumer.")
    # Write remaining batch to CSV
    if batch:
        with open(csv_file, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(batch)
finally:
    consumer.close()
    print("Consumer closed.")
