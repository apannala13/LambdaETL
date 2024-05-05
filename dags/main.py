import requests
import pandas as pd
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def api_to_s3(category, min_id):
    api_token = Variable.get("api_token")
    base_url = 'https://finnhub.io/api/v1/news'
    headers = {"X-Finnhub-Token": api_token}
    params = {"category": category, "min_id": min_id}
    
    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json() 
        df = pd.DataFrame(data)
        assert not df.empty, "No data fetched"
        df.to_csv('test.csv', index=False, header=True)
        print('Data saved to test.csv')
    except Exception as e:
        print(f"Error during API call: {e}")
        raise e

with DAG(
    dag_id='finnhub_end_to_end',
    start_date=pendulum.datetime(2024, 5, 4, tz="US/Pacific"),
    schedule_interval="0 9 * * *",  # 9am everyday
    concurrency=3,
    catchup=False,
    tags=['finnhub'],
    default_args=default_args,
) as dag:

    sync_start = DummyOperator(task_id='start_sync')
    sync_end = DummyOperator(task_id='end_sync')

    with TaskGroup(group_id='ingest_from_api', default_args={"pool": "sequential"}) as api_ingestion_group:
        api_ingest_task = PythonOperator(
            task_id='api_ingestion_task',
            python_callable=api_to_s3,
            op_kwargs={'category':'forex','min_id': 10}
        )

    sync_start >> api_ingestion_group >> sync_end
