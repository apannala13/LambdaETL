# StreamFlowETL
Hybrid ETL pipelines (batch/stream) leveraging Airflow and Kafka, based on Lambda architecture.

Initial architecture:
Ingestion from an API using Airflow -> DBT data models -> Snowflake warehouse.
Websocket -> Kafka Producer -> Flink consumer -> Cassandra DB for writes -> Snowflake for analytics

Ochestrated with Docker and Kubernetes.
