# Kafka-Spark-Streaming-Elastic-Kibana

This project demonstrates a real-time data pipeline using Kafka, Spark Streaming, Elasticsearch, and Kibana. It processes streaming data, applies censorship, and visualizes the results.

## Components

1. **Kafka**: Acts as the message broker for streaming data.
2. **Spark Streaming**: Processes the data in real-time.
3. **Elasticsearch**: Stores the processed data for querying.
4. **Kibana**: Visualizes the data stored in Elasticsearch.

## Prerequisites

- Docker and Docker Compose
- Python 3.x
- Apache Spark
- Kafka
- Elasticsearch and Kibana

## Setup Instructions

1. Clone the repository:
   ```bash
   git clone https://github.com/MaksymKashuba/kafka-spark-streaming-elastic-kibana.git
   cd kafka-spark-streaming-elastic-kibana
   ```

2. Configure environment variables in the `.env` file:
   - Update the Kafka broker, topic, and other configurations as needed.

3. Start the services using Docker Compose:
   ```bash
   docker-compose up -d
   ```

4. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Run the Kafka producer:
   ```bash
   python producer.py
   ```

6. Run the Spark Streaming application:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3 SparkStreaming.py
   ```

## File Descriptions

- **`producer.py`**: Generates and sends messages to Kafka.
- **`SparkStreaming.py`**: Processes Kafka messages, applies censorship, and writes to Elasticsearch.
- **`docker-compose.yml`**: Defines the services for Kafka, Zookeeper, Elasticsearch, and Kibana.
- **`censored.txt`**: Contains words to be censored.
- **`dictionary.txt`**: Contains words used by the producer.

## Visualization

1. Access Kibana at `http://localhost:5601`.
2. Configure an index pattern for the Elasticsearch index.
3. Create visualizations and dashboards based on the processed data.

## Notes

- Ensure that the Kafka topic specified in the `.env` file matches the topic created in the `docker-compose.yml`.
- Modify the `censored.txt` file to update the list of censored words.

## License

This project is licensed under the MIT License.
