from kafka import KafkaConsumer
import json

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topics = ['topic1', 'topic2']

# Create Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',  # Read from the beginning of the topic
                         enable_auto_commit=True,  # Automatically commit offsets
                         group_id='my_consumer_group')  # Consumer group ID

# Subscribe to topics
consumer.subscribe(topics)
try:
    # Continuously consume messages
    for message in consumer:
        # Extract topic number from topic name
        topic_number = int(message.topic[-1])
        
        # Print the raw message value for debugging
        print("Received message:", message.value)

        # Decode the message value
        try:
            # Remove the outer double quotes and decode the inner JSON string
            inner_json_str = message.value.decode('utf-8').strip('"')
            value = json.loads(inner_json_str)
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
            continue  # Skip to the next message

        # Extract track information
        track_id = value.get('track_id')
        track_title = value.get('track_title')
        artist_name = value.get('artist_name')
        
        # Print topic number, track name, track ID, and artist name
        print(f"Topic: {topic_number}, Track Name: {track_title}, Track ID: {track_id}, Artist Name: {artist_name}")

except KeyboardInterrupt:
    # Close the consumer on interrupt (Ctrl+C)
    consumer.close()


