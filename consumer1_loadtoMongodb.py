from confluent_kafka import Consumer, KafkaError
import json
from pymongo import MongoClient

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'audio_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading at the earliest offset
}

# MongoDB configuration
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['audio_features_db']
collection = db['audio_features']

# Kafka topic to read data from
topic = 'audiofeatures'

# Create Kafka consumer
consumer = Consumer(conf)
consumer.subscribe([topic])


try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new messages
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                print(f'Warning: {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Process message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                file_name = data['file_name']
                features = data['features']
                # Insert data into MongoDB collection
                collection.insert_one({'file_name': file_name, 'features': features})
                print(f'Data from {file_name} inserted into MongoDB')
            except Exception as e:
                print(f'Error processing message: {e}')
finally:
    # Clean up
    consumer.close()
    mongo_client.close()