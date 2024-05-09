import pandas as pd
import os
import librosa
import numpy as np
import json
from confluent_kafka import Producer

# Kafka producer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
}

# Create Kafka producer
producer = Producer(kafka_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Read the CSV file
df = pd.read_csv("raw_tracks.csv")

# Select the desired columns
selected_columns = ['track_id', 'album_id', 'artist_id', 'artist_name', 'album_title', 'track_title', 'track_duration', 'track_genres', 'track_listens', 'track_favorites']
song_info_df = df[selected_columns]

# Define the parent folder containing audio files
parent_folder = r"fma_large"

# Define a function to extract features from audio files
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

# Create a list to store filenames and their corresponding features
audio_features_data = []

# Iterate over subfolders
for folder_name in os.listdir(parent_folder):
    folder_path = os.path.join(parent_folder, folder_name)
    if os.path.isdir(folder_path):
        # Iterate over audio files in the subfolder
        for filename in os.listdir(folder_path):
            if filename.endswith(".mp3"):  # Assuming audio files are in MP3 format
                audio_file = os.path.join(folder_path, filename)
                # Extract features from the audio file
                features = extract_features(audio_file)
                if features is not None:
                    # Remove leading zeros and ".mp3" extension from filename
                    modified_filename = filename.lstrip("0").replace(".mp3", "")
                    # Get the corresponding row from song_info_df based on track_id
                    track_info = song_info_df.loc[song_info_df['track_id'] == int(modified_filename)].to_dict(orient='records')[0]
                    # Create a dictionary containing song information, audio features, and selected columns
                    audio_features_data.append({'track_id': int(modified_filename), 
                                                'audio_features': features,
                                                **track_info})
                else:
                    print(f"Failed to extract audio features for file '{filename}'.")
            else:
                print(f"Skipping non-MP3 file '{filename}'.")

# Send data to Kafka topic
for audio_data in audio_features_data:
    producer.produce('audios', json.dumps(audio_data), callback=delivery_report)

# Flush messages to Kafka
producer.flush()
