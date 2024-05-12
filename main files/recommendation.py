from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import from_json, col, udf, size
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, StringType, IntegerType
from pyspark.ml.linalg import Vectors, VectorUDT
from kafka import KafkaProducer
import json
import time

#spark session
spark = SparkSession.builder \
    .appName("KafkaStreamToDataFrame") \
    .getOrCreate()

#kafka servers and topics
bootstrap_servers = "localhost:9092"
input_topic = "recommendationsystem"
output_topics = ["topic1", "topic2", "topic3", "topic4", "topic5"]

#defining schema for JSON file
schema = StructType([
    StructField("id", StringType()),
    StructField("track_id", StringType()),
    StructField("audio_features", ArrayType(DoubleType())),
    StructField("album_id", StringType()),
    StructField("artist_id", StringType()),
    StructField("artist_name", StringType()),
    StructField("album_title", StringType()),
    StructField("track_title", StringType()),
    StructField("track_duration", StringType()),
    StructField("track_genres", StringType()),
    StructField("track_listens", IntegerType()),
    StructField("track_favorites", IntegerType())
])

#kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

#batch size
batch_size = 20

#loop to continue until all messages recieved
while True:
    #suscribe to reccomdationsystem
    print(f"Reading {batch_size} messages from Kafka...")
    kafka_batch_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("maxOffsetsPerTrigger", batch_size) \
        .load()

    #Parse JSON messages
    print("Parsing JSON messages...")
    parsed_batch_df = kafka_batch_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Define a UDF to convert array of doubles to Vector
    to_vector_udf = udf(lambda arr: Vectors.dense(arr), VectorUDT())

    # Filter out rows with null audio features
    parsed_batch_filtered = parsed_batch_df.filter(col("audio_features").isNotNull())

    # Filter out rows with empty arrays
    parsed_batch_filtered = parsed_batch_filtered.filter(size(col("audio_features")) > 0)

    # Convert array of doubles to Vector
    parsed_batch_filtered = parsed_batch_filtered.withColumn("features", to_vector_udf(col("audio_features")))

    # Assemble features into a single vector
    assembler = VectorAssembler(inputCols=["features"], outputCol="features_vector")
    feature_batch_df = assembler.transform(parsed_batch_filtered)

    #kmeans clustering
    kmeans = KMeans(k=5, seed=1) 
    model = kmeans.fit(feature_batch_df)

    # Assigning each track to a cluster
    clustered_batch_df = model.transform(feature_batch_df)

    #Distributing tracks into topics based on clusters
    for i in range(5):
        topic_batch_df = clustered_batch_df.filter(col("prediction") == i).select("track_id", "track_title", "artist_name", "track_favorites", "track_genres")
        topic_name = output_topics[i] if i < len(output_topics) else f"topic{i + 1}"
        print(f"Topic {i + 1} - {topic_name}:")
        topic_batch_df.show(truncate=False)

        #send to Kafka topics
        records = topic_batch_df.toJSON().collect()
        for record in records:
            print("message delivered")
            producer.send(topic_name, value=record)
            