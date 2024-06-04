The application folder contains a sample output for the model as a flask applicatio. Download the folder and run app1.py to see :D

# Project Description
Our project aims to develop a recommendation system akin to Spotify's, leveraging Apache Spark and Kafka for data processing and real-time streaming. By harnessing the power of big data technologies, we seek to provide users with personalized music recommendations based on their preferences

## Phase 1: Extract, Transform, Load (ETL) Pipeline

### Data Sources:
- **CSV Metadata**: We gathered metadata about tracks from CSV files, selecting only the relevant columns required for our recommendation system.
- **Audio Files**: Our audio feature extraction process involved traversing through directories containing audio files. Leveraging the `librosa` library, we extracted key audio features such as spectrograms, mel-frequency cepstral coefficients (MFCCs), and chroma features.

### Workflow:
1. **CSV Metadata Processing**:
   - We utilized Python's `pandas` library to read and process the CSV metadata files, ensuring we captured all necessary information for our recommendation model.

2. **Audio Feature Extraction**:
   - Employing the `os` library, we navigated through directories housing audio files, retrieving them based on their track IDs.
   - Leveraging the powerful feature extraction capabilities of `librosa`, we extracted a wide array of audio features from each audio file.

3. **Data Integration**:
   - Merging the extracted audio features with the relevant metadata, we created a comprehensive DataFrame that served as the foundation for our recommendation system's dataset.

4. **Apache Kafka Implementation**:
   - **Producer**: We serialized and combined dataframes, producing Kafka records containing the merged data.
   - **Consumer**: In the consumer component, we consumed Kafka records in real-time, concurrently uploading the data to MongoDB for storage.

### Tools Used:
- **Python Libraries**: `pandas`, `os`, and `librosa` were pivotal in processing CSV metadata, navigating directories, and extracting audio features, respectively.
- **Apache Kafka**: Facilitated real-time data streaming, enabling seamless communication between the producer and consumer components.
- **MongoDB**: Served as the database backend, providing storage for our processed data.

## Phase 2: Music Recommendation Model

In this phase, we focused on building the recommendation model for our system. Here's a breakdown of what we achieved:

### Producer Setup:
- Loaded the data we saved in MongoDB.
- Sent the data to a consumer component for further processing.
- There were 2 algorithms being used to achieve the recommendation model:
- 1- K-MEANS CLUSTERING: the entire dataset is divided into K clusters (depending on size of sample and size of bands/buckets). Songs in the same cluster are recommendations for each other.
- 2- COLLABORATIVE FILTERING: The clusters from K-Means are then filtered more to obtain more 'clusters within clusters'. This gives the most popular songs which will be displayed on the main homepage of the SPOTIFY application. 

### Schema Definition:
- Defined a schema to upload the data into a Spark DataFrame.

### Model Development:
- Utilized the Spark MLlib library for model development.
- Implemented a Vector Assembler to prepare the features.
- Employed K-means clustering to group similar audio features.

### Data Distribution:
- Distributed our data into multiple Kafka topics for efficient processing.

### Kafka Integration:
- Included a producer in the same script to upload data into these Kafka topics seamlessly.

### Tools Used:
- **Apache Spark**: Leveraged for distributed computing and model development.
- **MongoDB**: Initial data source for training the recommendation model.
- **Spark MLlib**: Used for machine learning tasks such as feature transformation and clustering.
- **Apache Kafka**: Facilitated real-time data streaming and efficient data distribution.

## Phase 3: Deployment

In Phase 3, our focus shifted towards deploying our recommendation system and providing a user-friendly interface. Here's what we accomplished:

### Consumer Setup:
- Developed a consumer that reads the contents of the Kafka topics.
- The consumer gives songs arranged in text files, and one file which contains the most popular songs (obtained by collaborative filtering)
- The most popular songs are shown on the main SPOTIFY homepage, and which ever song is selected, the relevent recommendation are fetched and displayed from its relevent text file and relevent section in text file.

### Web Application Development:
- Created a web application using Flask.
- The web application displays the first few songs of each topic to the user.

### User Interaction:
- Implemented functionality for users to select their preferred topic.
- Upon selection, the web application displays all songs within the chosen topic.

### Tools Used:
- **Flask**: Used for developing the web application and handling user interactions.
- **Apache Kafka**: Continued usage for real-time data streaming and communication between components.
