from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from pyspark.ml.feature import StringIndexer

from confluent_kafka import Consumer, KafkaError
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MusicRecommendationSystemConsumer") \
    .getOrCreate()

# Define schema for Kafka message
schema = StructType([
    StructField("file_path", StringType(), True),
    StructField("features", ArrayType(DoubleType()), True)
])
# Create Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'music_recommendation_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['recommendationsystem'])


# Function to parse Kafka message
def parse_message(msg):
    try:
        value = msg.value().decode('utf-8')
        # Replace single quotes with double quotes to make it valid JSON
        value = value.replace("'", "\"")
        parsed_value = json.loads(value)
        return parsed_value
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None

# Function to train recommendation model
def train_model(data):
    # Convert features array to dense vector
    parsed_data = spark.createDataFrame(data, schema=schema)

    # Use StringIndexer to convert file_path to numeric IDs
    indexer = StringIndexer(inputCol="file_path", outputCol="user_id")
    indexed_data = indexer.fit(parsed_data).transform(parsed_data)

    print("Indexed Data Schema:")
    indexed_data.printSchema()
    indexed_data.show(5)

    # Split the data into training and testing sets
    (training, test) = parsed_data.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="file_path", ratingCol="features",
              coldStartStrategy="drop")
    model = als.fit(training)

    # Evaluate the model by computing RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="features",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) = " + str(rmse))

    # Generate top 10 music recommendations for each user
    userRecs = model.recommendForAllUsers(10)

    # Show recommendations
    userRecs.show(truncate=False)

# Consume messages and train model
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            value = parse_message(msg)
            if value is not None:
                train_model(value)
except KeyboardInterrupt:
    pass
finally:
    # Close Kafka consumer
    consumer.close()
    # Stop Spark session
    spark.stop()