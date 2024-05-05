import json
import librosa
import numpy as np
import os
from confluent_kafka import Producer

from sklearn.preprocessing import StandardScaler, MinMaxScaler

def extract_features(file_path):
    try:
        y, sr = librosa.load(file_path)
        mfcc = librosa.feature.mfcc(y=y, sr=sr)
        spectral_centroid = librosa.feature.spectral_centroid(y=y, sr=sr)
        zero_crossing_rate = librosa.feature.zero_crossing_rate(y)
        mfcc_mean = np.mean(mfcc, axis=1)
        mfcc_std = np.std(mfcc, axis=1)
        spectral_centroid_mean = np.mean(spectral_centroid)
        spectral_centroid_std = np.std(spectral_centroid)
        zero_crossing_rate_mean = np.mean(zero_crossing_rate)
        zero_crossing_rate_std = np.std(zero_crossing_rate)
        features = np.concatenate([mfcc_mean, mfcc_std, [spectral_centroid_mean, spectral_centroid_std], [zero_crossing_rate_mean, zero_crossing_rate_std]])
        return features.tolist()  # Convert to list
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None
    
def load_dataset(folder_path):
    X = []
    file_names = []
    for file in os.listdir(folder_path):
        if file.endswith('.mp3'):
            file_path = os.path.join(folder_path, file)
            features = extract_features(file_path)
            X.append(features)
            file_names.append(file)
    return X, file_names

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
}

# Create Kafka producer
producer = Producer(conf)

# Kafka topic to send data to
topic = 'audiofeatures'

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_to_kafka(file_path):
    features = extract_features(file_path)
    if features:
#        print("File:", os.path.basename(file_path))
 #       print("Features:", features)
        data = {
            'file_name': os.path.basename(file_path),
            'features': features
        }
        producer.produce(topic, json.dumps(data), callback=delivery_report)
        producer.flush()

# Path to the folder containing the MP3 files
folder_path = 'sample_audio_20gb'

# Load dataset
#X, file_names = load_dataset(folder_path)

# Normalization
#scaler = MinMaxScaler()
#X_normalized = scaler.fit_transform(X)

# Send data to Kafka
#send_to_kafka(X_normalized, file_names)

for file in os.listdir(folder_path):
    if file.endswith('.mp3'):
        file_path = os.path.join(folder_path, file)
        features = send_to_kafka(file_path)
        if features:
            print("Features of the first file:", features)
            break  # Stop after printing the features of the first file

print("Data sent to Kafka topic:", topic)
