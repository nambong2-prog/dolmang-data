"""
init_history.py  ─  최초 1회만 실행하는 과거 데이터 초기화 스크립트
────────────────────────────────────────────────────────────────────
- 오늘부터 최근 7영업일치 데이터를 API로 수집
- trend.json 에 날짜별 스냅샷 + daily_report 누적 저장
- data.json 은 오늘 데이터로 덮어씀 (fetch_data.py 와 동일)

사용법:
  python init_history.py          # 최근 7영업일 수집
  python init_history.py --days 14  # 최근 14영업일 수집
"""

import requests
import json
import os
import sys
from datetime import datetime, timedelta

# ── fetch_data.py 와 완전히 공유하는 설정 ──────────────────────
API_KEY = os.environ.get("API_KEY")
BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

MANGAM_VARIETIES = ["레드향", "천혜향", "한라봉", "카라향"]
HOBAK_ITEMS      = ["미니밤호박", "단호박"]
JEJU_ORIGINS     = ["제주"]
TARGET_LCLSF     = ["06"]
HISTORY_DAYS     = 60

# 수집할 영업일수 (기본 7일)
TARGET_BDAYS = 7
for arg in sys.argv[1:]:
    if arg.startswith("--days"):
        try:
            TARGET_BDAYS = int(arg.split("=")[-1] if "=" in arg else sys.argv[sys.argv.index(arg)+1])
        except:
            pass


# ── fetch_data.py 의 함수들을 그대로 복사 ─────────────────────

def fetch_all_pages(date_str):
    all_items = []
    for lclsf_cd in TARGET_LCLSF:
        page = 1
        while True:
            url = (
                f"{BASE_URL}?serviceKey={API_KEY}&returnType=JSON&numOfRows=1000"
                f"&pageNo={page}&cond[trd_clcln_ymd::EQ]={date_str}"
                f"&cond[gds_lclsf_cd::EQ]={lclsf_cd}"
            )
            try:
                resp = requests.get(url, timeout=30)
                data = resp.json()
                body = data.get("response", {}).get("body", {})
                total = body.get("totalCount", 0)
                items = body.get("items", {})
                if not items:
                    break
                item_list = items.get("item", [])
                if isinstance(item_list, dict):
                    item_list = [item_list]
                all_items.extend(item_list)
                if page * 1000 >= total:
                    break
                page += 1
            except Exception as e:
                print(f"  API 오류: {e}")
                break
    return all_items


def parse_item(item, category, group_key):
    vrty_nm = (item.get("corp_gds_vrty_nm") or "").strip()
    item_nm = (item.get("corp_gds_item_nm") or "").strip()
    plor_nm = (item.get("plor_nm") or "").strip()
    trd_se  = (item.get("trd_se") or "").strip()
    unit_qty = float(item.get("unit_qty") or 0)
    try:
        kg = float(unit_qty)
        spec = f"{int(kg)}kg" if kg > 0 and kg == int(kg) else (f"{kg}kg" if kg > 0 else (item.get("pkg_nm") or ""))
    except:
        spec = (item.get("pkg_nm") or "")
    return {
        "카테고리": category,
        "품종": vrty_nm if group_key == "품종" else item_nm,
        "품목": item_nm,
        "도매시장": (item.get("whsl_mrkt_nm") or ""),
        "법인": (item.get("corp_nm") or ""),
        "원산지": plor_nm,
        "규격": spec,
        "단위": (item.get("unit_nm") or ""),
        "단위수량": unit_qty,
        "경락가": int(float(item.get("scsbd_prc") or 0)),
        "거래량": int(float(item.get("qty") or 0)),
        "낙찰일시": (item.get("scsbd_dt") or ""),
        "매매방법": trd_se,
        "is_auction": "정가수의" not in trd_se,
    }


def filter_items(all_items):
    mangam_data, gamgyul_data, hobak_data = [], [], []
    for item in all_items:
        vrty_nm  = (item.get("corp_gds_vrty_nm") or "").strip()
        item_nm  = (item.get("corp_gds_item_nm") or "").strip()
        plor_nm  = (item.get("plor_nm") or "").strip()
        mclsf_cd = (item.get("gds_mclsf_cd") or "").strip()
        if mclsf_cd == "15" and vrty_nm in MANGAM_VARIETIES:
            mangam_data.append(parse_item(item, "만감류", "품종"))
        elif mclsf_cd != "15" and "감귤" in item_nm:
            gamgyul_data.append(parse_item(item, "감귤", "품종"))
        elif any(h in item_nm for h in HOBAK_ITEMS) and any(j in plor_nm for j in JEJU_ORIGINS):
            hobak_data.append(parse_item(item, "호박", "품목"))
    return mangam_data, gamgyul_data, hobak_data


def remove_outliers(recs):
    if len(recs) < 3:
        return recs
    prices = sorted([r["price"] for r in recs])
    n = len(prices)
    med = prices[n // 2] if n % 2 == 1 else (prices[n//2-1] + prices[n//2]) / 2
    return [r for r in recs if 0.3 * med <= r["price"] <= 3.5 * med]


def make_daily_report(rows, group_key):
    """품종+규격별 일일 통계 (경매만)"""
    auction = [r for r in rows if r.get("is_auction")]
    summary = {}
    for row in auction:
        k = f"{row[group_key]}_{row['규격']}"
        if k not in summary:
            summary[k] = {
                "품종": row[group_key], "규격": row["규격"],
                "총액": 0, "총거래량": 0, "총중량": 0.0,
                "최고가": 0, "최저가": 9999999, "records": []
            }
        p = row["경락가"]; q = row["거래량"]; uq = float(row.get("단위수량") or 0)
        summary[k]["총액"]     += p * q
        summary[k]["총거래량"] += q
        summary[k]["총중량"]   += q * uq
        if p > summary[k]["최고가"]: summary[k]["최고가"] = p
        if p < summary[k]["최저가"]: summary[k]["최저가"] = p
        summary[k]["records"].append({"price": p, "qty": q})

    result = []
    for v in summary.values():
        if v["총거래량"] > 0:
            v["평균가"] = round(v["총액"] / v["총거래량"])
            recs = sorted(v["records"], key=lambda x: -x["price"])
            n50  = max(1, len(recs) // 2)
            top50 = recs[:n50]
            t50q  = sum(r["qty"] for r in top50)
            v["상위50평균가"]  = round(sum(r["price"]*r["qty"] for r in top50) / t50q) if t50q else v["평균가"]
            v["상위50기준가"]  = top50[-1]["price"] if top50 else v["평균가"]
            v["상위50거래량"]  = sum(r["qty"] for r in top50)
            del v["records"]
            result.append(v)
    return result


def make_trend_snapshot(data_list, group_key, date_str):
    auction = [r for r in data_list if r.get("is_auction")]
    summary = {}
    for row in auction:
        f  = row[group_key]; s = row["규격"]; p = row["경락가"]
        q  = row["거래량"];  uq = float(row.get("단위수량") or 0)
        key = f"{f}_{s}"
        if key not in summary:
            summary[key] = {"품종": f, "규격": s, "총액": 0, "총거래량": 0, "총중량": 0.0}
        summary[key]["총액"]     += p * q
        summary[key]["총거래량"] += q
        summary[key]["총중량"]   += q * uq
    result = []
    for v in summary.values():
        if v["총거래량"] > 0:
            result.append({
                "날짜": date_str, "품종": v["품종"], "규격": v["규격"],
                "평균가": round(v["총액"] / v["총거래량"]),
                "총거래량": v["총거래량"], "총중량": round(v["총중량"], 1),
            })
    return result


def load_trend(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def save_trend(path, trend):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(trend, f, ensure_ascii=False, indent=2)


def update_trend(trend, cat, snapshots, date_str, daily_report=None):
    if cat not in trend:
        trend[cat] = {}
    trend[cat][date_str] = snapshots
    if daily_report is not None:
        dkey = cat + "_daily"
        if dkey not in trend:
            trend[dkey] = {}
        trend[dkey][date_str] = daily_report
        dates_d = sorted(trend[dkey].keys())
        if len(dates_d) > HISTORY_DAYS:
            for old in dates_d[:-HISTORY_DAYS]:
                del trend[dkey][old]
    dates = sorted(trend[cat].keys())
    if len(dates) > HISTORY_DAYS:
        for old in dates[:-HISTORY_DAYS]:
            del trend[cat][old]
    return trend


# ── 메인: 최근 N 영업일 수집 ──────────────────────────────────

def get_recent_bdays(n):
    """오늘 포함 최근 n 영업일 날짜 목록 (오래된 순)"""
    result = []
    check = datetime.now()
    while len(result) < n:
        if check.weekday() < 5:   # 월~금
            result.append(check.strftime("%Y-%m-%d"))
        check -= timedelta(days=1)
    return list(reversed(result))   # 오래된 날짜 먼저


dates_to_fetch = get_recent_bdays(TARGET_BDAYS)
print(f"수집 대상 날짜 ({TARGET_BDAYS}영업일): {dates_to_fetch}")

trend = load_trend("trend.json")
today = datetime.now().strftime("%Y-%m-%d")

for date_str in dates_to_fetch:
    # 이미 오늘 데이터가 trend에 있으면 skip (중복 방지)
    if date_str in trend.get("mangam", {}):
        print(f"  {date_str} - trend 이미 존재, skip")
        continue

    print(f"\n[{date_str}] 수집 중...")
    all_items = fetch_all_pages(date_str)
    print(f"  수집: {len(all_items)}건")

    mangam_data, gamgyul_data, hobak_data = filter_items(all_items)
    print(f"  만감류:{len(mangam_data)} 감귤:{len(gamgyul_data)} 호박:{len(hobak_data)}")

    if not mangam_data and not gamgyul_data:
        print(f"  {date_str} 데이터 없음 (공휴일/경매없음) - skip")
        continue

    # trend.json 업데이트
    mangam_snap  = make_trend_snapshot(mangam_data,  "품종", date_str)
    gamgyul_snap = make_trend_snapshot(gamgyul_data, "품종", date_str)
    hobak_snap   = make_trend_snapshot(hobak_data,   "품목", date_str)

    mangam_daily  = make_daily_report(mangam_data,  "품종")
    gamgyul_daily = make_daily_report(gamgyul_data, "품종")
    hobak_daily   = make_daily_report(hobak_data,   "품목")

    trend = update_trend(trend, "mangam",  mangam_snap,  date_str, mangam_daily)
    trend = update_trend(trend, "gamgyul", gamgyul_snap, date_str, gamgyul_daily)
    trend = update_trend(trend, "hobak",   hobak_snap,   date_str, hobak_daily)
    print(f"  {date_str} → trend.json 저장 완료")

save_trend("trend.json", trend)
print(f"\n✅ trend.json 저장 완료 (mangam: {len(trend.get('mangam', {}))}일치)")
print("이제 fetch_data.py 를 실행해서 오늘 data.json 도 갱신하세요.")
