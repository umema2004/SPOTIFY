# Project Description
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
