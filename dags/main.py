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
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def upload_to_s3(bucket_name, object_name, **kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='api_ingestion_task', key='file_path') #pull file
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print("file uploaded successfully")
    except NoCredentialsError:
        print("error: creds not available")
    except Exception as e:
        raise e
        
def load_data_into_snowflake(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    sql = """
    COPY INTO finnhub_stock_news_tbl
    FROM @finnhub_stage/all_stocks_data.csv
    FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', FIELD_DELIMITER = ',', SKIP_HEADER = 1)
    """
    try:
        hook.run(sql)
        print("data loaded into table")
    except Exception as e:
        raise e 

def api_ingestion(min_id, **kwargs):
    ti = kwargs['ti']
    api_token = Variable.get('api_token')
    stock_symbols = [
        'AAPL', 'MSFT', 'GOOGL','AMZN', 'TSLA', 'FB', 'BRK.A', 'V', 'JPM', 'JNJ', 'WMT', 'PG', 'BAC', 'XOM', 
        'CVX', 'KO', 'PFE', 'NFLX', 'T', 'CSCO', 'NKE', 'LLY',  'BA', 'ORCL', 'INTC',  'PEP',  'MCD', 'ABT', 'BMY', 'DIS'  
    ]
    combined_df = pd.DataFrame()  #empty df to hold all data from ALL stock tickers
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
            raise e
    if not combined_df.empty:
        file_path = 'all_stocks_data.csv'
        combined_df.to_csv(file_path, index=False, header=True)
        print('saved to csv file')
        ti.xcom_push(key = 'file_path', value = file_path) #pass file 
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
    #TO DO: add DBT model implementation
    #useful for taskgroup syncing to follow acyclic properties
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
                'bucket_name': Variable.get('s3_bucket'),
                'object_name': Variable.get('s3_object_name')
            }
        )

        load_snowflake = PythonOperator(
            task_id='load_data_into_snowflake',
            python_callable=load_data_into_snowflake
        )

        api_ingest_task >> file_to_s3 >> load_snowflake

    sync_start >> api_ingestion_group >> sync_end
