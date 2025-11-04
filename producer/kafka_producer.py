import argparse
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def wait_for_kafka(bootstrap_servers, max_retries=30, retry_interval=2):
    """Wait for Kafka to be available before proceeding."""
    print(f"Waiting for Kafka to be available at {bootstrap_servers}...")

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
            print("Kafka is available!")
            return True
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{max_retries}: Kafka not available yet, retrying...")
            time.sleep(retry_interval)

    raise Exception("Failed to connect to Kafka after maximum retries")

def send_data_to_kafka(args):
    # Configuration
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    kafka_topic = 'teams'
    if args.type == "data-2":
        kafka_topic = 'events'

    data_file_path = '/app/data/data-packet-1.json'
    if args.type == "data-2":
        data_file_path = '/app/data/data-packet-2.json'

    print(f"Kafka Bootstrap Servers: {kafka_bootstrap_servers}")
    print(f"Kafka Topic: {kafka_topic}")
    print(f"Data File Path: {data_file_path}")

    # Wait for Kafka to be available
    wait_for_kafka(kafka_bootstrap_servers)

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

    try:
        # Read the JSON file
        print(f"\nReading data from {data_file_path}...")
        with open(data_file_path, 'r') as f:
            data = json.load(f)


        total_messages = args.requests_per_second * args.duration
        sleep_time = 1 / args.requests_per_second
        # Send each team to Kafka
        for idx in range(0, total_messages):
            print(f"\nSending request {idx}/{total_messages}")

            # Send the message
            future = producer.send(kafka_topic, value=data)

            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)

            print(f"  ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
            
            time.sleep(sleep_time)

        # Ensure all messages are sent
        producer.flush()
        print(f"\n✓ Successfully sent all {total_messages} teams to Kafka topic '{kafka_topic}'")

    except FileNotFoundError:
        print(f"Error: Data file not found at {data_file_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in data file: {e}")
        raise
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
        raise
    finally:
        producer.close()
        print("\nKafka producer closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog='Producer',
                    description='Sends Messages to kafka for testing purposes'
                    )

    parser.add_argument('-r', '--requests_per_second', type=int, default=5)
    parser.add_argument('-d', '--duration', type=int, default=30)
    parser.add_argument('-t', '--type', default="data-1")

    args = parser.parse_args()
    print(args)
    send_data_to_kafka(args)
