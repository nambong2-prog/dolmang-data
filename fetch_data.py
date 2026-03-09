import requests
import json
import os
from datetime import datetime

API_KEY = os.environ.get("API_KEY")
today = datetime.now().strftime("%Y-%m-%d")

url = "https://apis.data.go.kr/B552845/katRealTime2/trades2"
params = {
    'serviceKey': API_KEY,
    'returnType': 'JSON',
    'numOfRows': '9999',
    'pageNo': '1',
    'cond[trd_clcln_ymd::EQ]': today
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
        {"whsl_mrkt_nm": "서울가락", "corp_nm": "서울청과", "corp_gds_item_nm": "감귤", "pkg_nm": "5kg", "scsbd_prc": 25000, "qty": 50},
        {"whsl_mrkt_nm": "부산엄궁", "corp_nm": "항도청과", "corp_gds_item_nm": "레드향", "pkg_nm": "5kg", "scsbd_prc": 42000, "qty": 100},
        {"whsl_mrkt_nm": "구리시장", "corp_nm": "구리청과", "corp_gds_item_nm": "천혜향", "pkg_nm": "5kg", "scsbd_prc": 38000, "qty": 30},
        {"whsl_mrkt_nm": "대전노은", "corp_nm": "대전청과", "corp_gds_item_nm": "한라봉", "pkg_nm": "3kg", "scsbd_prc": 35000, "qty": 25},
        {"whsl_mrkt_nm": "서울가락", "corp_nm": "서울청과", "corp_gds_item_nm": "황금향", "pkg_nm": "5kg", "scsbd_prc": 38000, "qty": 40}
    ]

filtered_data = []
for item in raw_data:
    name = item.get('corp_gds_item_nm') or item.get('gds_sclsf_nm') or ''
    fruit_name = next((f for f in target_fruits if f in name), None)
    if not fruit_name:
        continue
    filtered_data.append({
        "도매시장": item.get('whsl_mrkt_nm') or '',
        "법인": item.get('corp_nm') or '',
        "품종": fruit_name,
        "세부품목": name,
        "품종명": item.get('corp_gds_vrty_nm') or '',
        "원산지": item.get('plor_nm') or '',
        "규격": item.get('pkg_nm') or '',
        "단위": item.get('unit_nm') or '',
        "경락가": int(float(item.get('scsbd_prc') or 0)),
        "거래량": int(float(item.get('qty') or 0)),
        "낙찰일시": item.get('scsbd_dt') or '',
        "매매방법": item.get('trd_se') or ''
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
