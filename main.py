
import requests 
import pandas as pd 
from datetime import datetime, timedelta 
import pendulum 
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

#write csv to s3 using s3 hook
#s3 to snowflake operator 

default_args={
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

api_token = Variable.get('api_token')
min_id = 10 

def api_to_s3(api_key, min_id):
    base_url = 'https://finnhub.io/api/v1/news'

    headers = {
        "X-Finnhub-Token":api_key
    }
    params = {
        "category": 'forex',
    }
    if min_id is not None:
        params["min_id"] = min_id
        
    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json() 
        df = pd.DataFrame(data)
        assert not df.empty, "no data fetched"

        df.to_csv('test.csv', index=False, header=True)
        print(f'data saved to test.csv')
    except Exception as e:
        raise e            


with DAG(
    dag_id = f'finnhub_end_to_end',
    start_date = pendulum.datetime(2024, 5, 4, tz="US/Pacific"),
    schedule_interval = "0 9 * * *",
    concurrency = 3,
    catchup = False,
    tags = ['finnhub'],
    default_args = default_args,
) as dag:
    
    sync0 = DummyOperator(task_id='sync0_task_finnhub',trigger_rule = 'all_done',dag=dag)
    sync1 = DummyOperator(task_id='sync1_task_finnhub',trigger_rule = 'all_done',dag=dag)

    with TaskGroup(group_id='ingest_from_api', default_args={"pool": "sequential"}) as api_ingestion_group:
        t1 = PythonOperator(
            task_id = 'ingest_from_api',
            python_callable = api_to_s3
        ) 

    sync0 >> api_ingestion_group >> sync1

