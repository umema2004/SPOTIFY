import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Kafka consumer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'mongo_uploader_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

# MongoDB configuration
mongo_conf = {
    'host': 'localhost',  # MongoDB host
    'port': 27017,  # MongoDB port
    'database': 'audio_features',  # MongoDB database name
    'collection': 'audios'  # MongoDB collection name (updated to match the topic name)
}

# Create Kafka consumer
consumer = Consumer(kafka_conf)

# Subscribe to Kafka topic
consumer.subscribe(['audios'])  # Updated to subscribe to 'audios' topic

# Create MongoDB client
mongo_client = MongoClient(mongo_conf['host'], mongo_conf['port'])
db = mongo_client[mongo_conf['database']]
collection = db[mongo_conf['collection']]

# Consume messages and upload to MongoDB
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Process message
        audio_data = json.loads(msg.value().decode('utf-8'))
        collection.insert_one(audio_data)
except KeyboardInterrupt:
    pass

finally:
    # Close Kafka consumer
    consumer.close()
