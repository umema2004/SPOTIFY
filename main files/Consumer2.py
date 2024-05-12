from pymongo import MongoClient
from confluent_kafka import Producer
import json
from bson import json_util

MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
MONGO_DB_NAME = "audio_features"
MONGO_COLLECTION_NAME = "audios"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "recommendationsystem"

mongo_client = MongoClient(MONGO_CONNECTION_STRING)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_collection = mongo_db[MONGO_COLLECTION_NAME]

kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Fetch data from MongoDB and publish to Kafka
for document in mongo_collection.find():
    # Convert document to JSON and send to Kafka
    kafka_producer.produce(KAFKA_TOPIC, json.dumps(document, default=json_util.default).encode('utf-8'), callback=delivery_report)

# Wait for all messages to be delivered
kafka_producer.flush()
