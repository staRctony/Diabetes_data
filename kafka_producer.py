import csv
from confluent_kafka import Producer
import json

# Kafka configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092'  # Replace with your Kafka server if different
}
producer = Producer(producer_config)
topic_name = 'diabetes_topic'

# Callback for message delivery confirmation
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Path to your CSV file
csv_file_path = '/Users/sudhir/Desktop/Fall2024/BIGDATA/Project/diabetes_dataset.csv'

try:
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            message = json.dumps(row)  # Convert the row to JSON
            producer.produce(topic_name, value=message, callback=delivery_report)
            producer.flush()  # Ensure message is sent
        print("All rows sent to Kafka.")
except Exception as e:
    print(f"Error while reading CSV or sending data: {e}")

