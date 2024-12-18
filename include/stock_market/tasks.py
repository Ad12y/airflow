import requests
import json
from io import BytesIO
from minio import Minio 
from airflow.hooks.base import BaseHook
def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers = api.extra_dejson['header'])
    return json.dumps(response.json()['chart']['result'][0]) 

def _store_prices(stock, symbol):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split("//")[1],
        access_key = minio.login,
        secret_key = minio.password, 
        secure = False
    )
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name = bucket_name,
        object_name = f'{symbol}/prices.josn', 
        data = BytesIO(data), 
        length = len(data)
    )
    return f'{objw.bucket_name}/{symbol}'



