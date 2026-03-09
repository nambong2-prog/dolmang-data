import requests
import json
import os
from datetime import datetime

API_KEY = os.environ.get("API_KEY")
today = datetime.now().strftime("%Y%m%d")

url = "https://apis.data.go.kr/B552845/katRealTime2/katRealTime2"
params = {
    'serviceKey': API_KEY,
    'saleDate': today,
    'productClsCode': '02',
    'categoryCode': '400',
    '_type': 'json',
    'numOfRows': '9999',
    'pageNo': '1'
}

target_fruits = ['감귤', '한라봉', '레드향', '천혜향', '황금향']

try:
    response = requests.get(url, params=params, timeout=10)
    print(f"응답코드: {response.status_code}")
    print(f"응답내용 앞부분: {response.text[:300]}")
    
    result = response.json()
    items = result['response']['body']['items']['item']
    if isinstance(items, dict):
        items = [items]
    raw_data = items
    print(f"API 성공: {len(raw_data)}건")

except Exception as e:
    print(f"API 실패 또는 휴일: {e}")
    raw_data = [
        {"mrktnm": "서울가락", "cprnm": "서울청과", "prdlstnm": "감귤", "stdunitnewnm": "5kg", "sbidprc": 25000, "del
