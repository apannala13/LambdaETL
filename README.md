# LambdaETL
Hybrid ETL pipelines (batch/stream) leveraging Airflow and Kafka, based on Lambda architecture.

Initial architecture:

Batch:

API ingestion with Airflow -> Snowflake warehouse -> DBT jobs

Stream:

Websocket -> Kafka Producer -> Flink consumer -> Cassandra DB for writes

Workflow will be ochestrated with Docker and Kubernetes.


