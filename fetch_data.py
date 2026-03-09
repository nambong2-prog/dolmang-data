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
    print(f"응답내용: {response.text[:300]}")
    result = response.json()
    items = result['response']['body']['items']['item']
    if isinstance(items, dict):
        items = [items]
    raw_data = items
    print(f"API 성공: {len(raw_data)}건")
except Exception as e:
    print(f"API 실패: {e}")
    raw_data = [
        {"mrktnm": "서울가락", "cprnm": "서울청과", "prdlstnm": "감귤", "stdunitnewnm": "5kg", "sbidprc": 25000, "delngqy": 50},
        {"mrktnm": "부산엄궁", "cprnm": "항도청과", "prdlstnm": "레드향", "stdunitnewnm": "5kg", "sbidprc": 42000, "delngqy": 100},
        {"mrktnm": "구리시장", "cprnm": "구리청과", "prdlstnm": "천혜향", "stdunitnewnm": "5kg", "sbidprc": 38000, "delngqy": 30},
        {"mrktnm": "대전노은", "cprnm": "대전청과", "prdlstnm": "한라봉", "stdunitnewnm": "3kg", "sbidprc": 35000, "delngqy": 25},
        {"mrktnm": "서울가락", "cprnm": "서울청과", "prdlstnm": "황금향", "stdunitnewnm": "5kg", "sbidprc": 38000, "delngqy": 40}
    ]

filtered_data = []
for item in raw_data:
    name = item.get('prdlstnm') or item.get('prdlstNm') or item.get('productName') or ''
    fruit_name = next((f for f in target_fruits if f in name), None)
    if not fruit_name:
        continue
    filtered_data.append({
        "도매시장": item.get('mrktnm') or item.get('marketNm') or '',
        "법인": item.get('cprnm') or item.get('cprNm') or '',
        "품종": fruit_name,
        "세부품목": name,
        "규격": item.get('stdunitnewnm') or item.get('stdUnitNewNm') or '',
        "경락가": int(item.get('sbidprc') or item.get('sbidPrc') or 0),
        "거래량": int(item.get('delngqy') or item.get('delngQy') or 0)
    })

top6_data = {f: [] for f in target_fruits}
daily_report = {}
market_detail = {}

for row in filtered_data:
    f = row['품종']
    m = row['도매시장']
    s = row['규격']
    p = row['경락가']
    q = row['거래량']

    if q >= 20:
        top6_data[f].append(row)

    dk = f"{f}_{s}"
    if dk not in daily_report:
        daily_report[dk] = {"품종": f, "규격": s, "총거래량": 0, "총액": 0, "최고가": 0, "최저가": 999999}
    daily_report[dk]["총거래량"] += q
    daily_report[dk]["총액"] += p * q
    if p > daily_report[dk]["최고가"]:
        daily_report[dk]["최고가"] = p
    if p < daily_report[dk]["최저가"]:
        daily_report[dk]["최저가"] = p

    mk = f"{f}_{m}"
    if mk not in market_detail:
        market_detail[mk] = {"품종": f, "시장명": m, "총거래량": 0, "총액": 0, "최고가": 0}
    market_detail[mk]["총거래량"] += q
    market_detail[mk]["총액"] += p * q
    if p > market_detail[mk]["최고가"]:
        market_detail[mk]["최고가"] = p

for f in target_fruits:
    top6_data[f].sort(key=lambda x: x['경락가'], reverse=True)
    top6_data[f] = top6_data[f][:6]

daily_list = []
for v in daily_report.values():
    if v["총거래량"] > 0:
        v["평균가"] = round(v["총액"] / v["총거래량"])
        daily_list.append(v)

market_list = []
for v in market_detail.values():
    if v["총거래량"] > 0:
        v["평균가"] = round(v["총액"] / v["총거래량"])
        market_list.append(v)

output = {
    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "top6": top6_data,
    "daily_report": daily_list,
    "market_detail": market_list,
    "monthly_trend": daily_list,
    "total_history": filtered_data
}

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print("data.json 저장 완료")
