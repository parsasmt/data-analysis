import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaException
from elasticsearch import Elasticsearch, helpers
import json

# Function to read CSV file and classify data
def process_csv_and_send_to_kafka(csv_file, kafka_topic_prefix):
    # Read CSV file into pandas DataFrame
    df = pd.read_csv(csv_file)

    # Group by 'category' field (change 'category' to your desired field)
    grouped = df.groupby('category')

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'client.id': 'python-producer'
    }

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'group.id': 'python-consumer',
        'auto.offset.reset': 'earliest'
    }

    # Elasticsearch configuration
    es = Elasticsearch(['localhost:9200'])  # Elasticsearch address

    # Create Kafka producer
    producer = Producer(producer_config)

    # Create Kafka consumer
    consumer = Consumer(consumer_config)

    # Function to send data to Kafka
    def send_to_kafka(topic, data):
        try:
            # Produce data to Kafka topic
            producer.produce(topic, json.dumps(data).encode('utf-8'))
            producer.flush()
            print(f"Produced data to Kafka topic: {topic}")
        except KafkaException as e:
            print(f"Failed to produce data to Kafka topic {topic}: {str(e)}")

    # Function to index data into Elasticsearch
    def index_to_elasticsearch(index_name, data):
        try:
            # Prepare actions for bulk indexing
            actions = [
                {
                    "_index": index_name,
                    "_source": doc
                }
                for doc in data
            ]

            # Perform bulk indexing
            helpers.bulk(es, actions)
            print(f"Indexed data into Elasticsearch index: {index_name}")
        except Exception as e:
            print(f"Failed to index data into Elasticsearch index {index_name}: {str(e)}")

    # Process each group and send to Kafka and Elasticsearch
    for category, group_data in grouped:
        kafka_topic = f"{kafka_topic_prefix}_{category}"
        index_name = f"{kafka_topic_prefix}_{category}_index"

        # Convert group_data to list of dictionaries (JSON-like)
        data_to_send = group_data.to_dict(orient='records')

        # Send data to Kafka
        send_to_kafka(kafka_topic, data_to_send)

        # Index data into Elasticsearch
        index_to_elasticsearch(index_name, data_to_send)

    # Close Kafka producer and consumer
    producer.close()
    consumer.close()

# Main function to execute the process
def main():
    # Example CSV file path
    csv_file = './organizations-10000.csv'

    # Prefix for Kafka topics
    kafka_topic_prefix = 'Iraq'

    # Process CSV and send data to Kafka and Elasticsearch
    process_csv_and_send_to_kafka(csv_file, kafka_topic_prefix)

main()
