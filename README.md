# StreamFlowETL
Hybrid ETL pipelines (batch/stream) leveraging Airflow and Kafka, based on Lambda architecture.

Initial architecture:

Batch:

API ingestion with Airflow -> DBT data models -> Snowflake warehouse.

Stream:

Websocket -> Kafka Producer -> Flink consumer -> Cassandra DB for writes

Ochestrated with Docker and Kubernetes.


