
import requests 
import pandas as pd 


def ingest_from_url(api_key, category, min_id):
    base_url = 'https://finnhub.io/api/v1/news'

    headers = {
        "X-Finnhub-Token":api_key
    }
    params = {
        "category": category,
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

category = 'forex'
api_key = None
min_id = 10

ingest_from_url(
    api_key,
    category,
    min_id
)
