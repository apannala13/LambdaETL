import requests
import pandas as pd
from datetime import timedelta
import pendulum
import boto3 
from botocore.exceptions import NoCredentialsError
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

def upload_to_s3(file_name, bucket_name, object_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print("file uploaded successfully")
    except NoCredentialsError:
        print("error: credentials not available")
    except Exception as e:
        print(f"error occurred: {e}") 


def api_ingestion(min_id):
    api_token = Variable.get('api_token')
    stock_symbols = [
        'AAPL', 'MSFT', 'GOOGL','AMZN', 'TSLA', 'FB', 'BRK.A', 'V', 'JPM', 'JNJ', 'WMT', 'PG', 'BAC', 'XOM', 
        'CVX', 'KO', 'PFE', 'NFLX', 'T', 'CSCO', 'NKE', 'LLY',  'BA', 'ORCL', 'INTC',  'PEP',  'MCD', 'ABT', 'BMY', 'DIS'  
    ]
    combined_df = pd.DataFrame()  
    for category in stock_symbols:
        base_url = 'https://finnhub.io/api/v1/news'
        headers = {"X-Finnhub-Token": api_token}
        params = {"category": category}
        if min_id is not None:
            params["min_id"] = min_id
        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                assert not df.empty(), "No data fetched"
                df['Symbol'] = category                 
                combined_df = pd.concat([combined_df, df], ignore_index=True)         
        except Exception as e:
            raise e ("erorr fetching data")
    if not combined_df.empty:
        combined_df.to_csv('all_stocks_data.csv', index=False, header=True)
        print('saved to csv file')
    else:
        print("no data fetched for any stock symbols")


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
            python_callable=api_ingestion,
            op_kwargs={'min_id': 10}
        )

        file_to_s3 = PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'file_name': 'all_stocks_data.csv',
                'bucket_name': Variable.get('s3_bucket'),
                'object_name': Variable.get('s3_object_name')
            }
        )

        api_ingest_task >> file_to_s3
        
    sync_start >> api_ingestion_group >> sync_end
