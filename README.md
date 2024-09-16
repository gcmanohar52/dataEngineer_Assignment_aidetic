# DataCo Real-Time Data Pipeline

## Overview

This project demonstrates a real-time data pipeline built for DataCo to ingest, process, and analyze clickstream data from a web application. The pipeline is designed to:
- Ingest data from Kafka.
- Store the raw data in a data store (HBase or another NoSQL database).
- Process and aggregate the data using Apache Spark.
- Store the aggregated results in MySQL.
- Use stored procedures in MySQL to query the data.

## Tools and Technologies
- Apache Kafka
- Apache Spark
- MySQL
- Python (Kafka Producer and Consumer)
- HBase (or alternative NoSQL database)

## Steps to Run

### 1. Set Up Kafka
Start Kafka and create a topic named `clickstream`:
```bash
bin/kafka-topics.sh --create --topic clickstream --bootstrap-server localhost:9092
