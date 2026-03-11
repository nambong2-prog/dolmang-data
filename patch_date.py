"""
특정 날짜 하나를 trend.json에 추가하는 패치 스크립트
GitHub Actions에서 수동으로 실행
"""
import requests
import json
import os
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
API_KEY = os.environ.get("API_KEY")
BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

# ★ 추가할 날짜
PATCH_DATE = "2026-03-10"

MANGAM_VARIETIES = ["레드향", "천혜향", "한라봉", "카라향"]
HOBAK_ITEMS      = ["미니밤호박", "단호박"]
JEJU_ORIGINS     = ["제주"]
TARGET_LCLSF     = ["06"]
HISTORY_DAYS     = 60


# ── fetch_data.py 와 동일한 함수들 ──────────────────────────

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
                    print(f"  {lclsf_cd}: 데이터 없음")
                    break
                item_list = items.get("item", [])
                if isinstance(item_list, dict):
                    item_list = [item_list]
                all_items.extend(item_list)
                print(f"  페이지 {page}: {len(item_list)}건 (전체 {total})")
                if page * 1000 >= total:
                    break
                page += 1
            except Exception as e:
                print(f"  API 오류: {e}")
                break
    return all_items


def classify(raw):
    mangam, gamgyul, hobak = [], [], []
    for item in raw:
        gds_nm   = item.get("gdsNm", "")
        orgn_nm  = item.get("orgnNm", "") or ""
        lsps_nm  = item.get("lspsNm", "") or ""
        method   = item.get("mmthdNm", "") or ""
        prc      = float(item.get("trdPrc", 0) or 0)
        qty      = float(item.get("trdQy",  0) or 0)
        dt       = item.get("trdDe", "") or ""
        mkt      = item.get("mtsNm", "") or ""
        unit     = item.get("trdUntNm", "") or ""
        unit_qty = float(item.get("trdUntQy", 0) or 0)

        is_jeju   = any(o in orgn_nm for o in JEJU_ORIGINS)
        is_auction = "경매" in method

        row = {
            "도매시장": mkt, "법인": lsps_nm, "원산지": orgn_nm,
            "규격": unit, "단위": "kg", "단위수량": unit_qty,
            "경락가": prc, "거래량": qty,
            "낙찰일시": dt, "매매방법": method, "is_auction": is_auction,
        }

        if any(v in gds_nm for v in MANGAM_VARIETIES) and is_jeju:
            for v in MANGAM_VARIETIES:
                if v in gds_nm:
                    row["품종"] = v; row["품목"] = v; row["카테고리"] = "만감류"
                    mangam.append(row); break
        elif "감귤" in gds_nm and is_jeju:
            row["품종"] = "온주밀감"; row["품목"] = "감귤"; row["카테고리"] = "감귤"
            gamgyul.append(row)
        elif any(h in gds_nm for h in HOBAK_ITEMS):
            for h in HOBAK_ITEMS:
                if h in gds_nm:
                    row["품종"] = h; row["품목"] = h; row["카테고리"] = "호박"
                    hobak.append(row); break

    return mangam, gamgyul, hobak


def remove_outliers(rows, price_key="경락가"):
    if len(rows) < 3:
        return rows
    prices = [r[price_key] for r in rows if r[price_key] > 0]
    if not prices:
        return rows
    median = sorted(prices)[len(prices)//2]
    return [r for r in rows if 0 < r[price_key] <= median * 3.5]


def make_stats(data, group_key):
    from collections import defaultdict
    groups = defaultdict(list)
    for r in data:
        groups[r.get(group_key,"")].append(r)

    daily_report = []
    for variety, rows in groups.items():
        specs = defaultdict(list)
        for r in rows:
            specs[r.get("규격","")].append(r)
        for spec, srows in specs.items():
            auction_rows = [r for r in srows if r.get("is_auction")]
            auction_rows = remove_outliers(auction_rows)
            if not auction_rows:
                continue
            prices = [r["경락가"] for r in auction_rows]
            qtys   = [r["거래량"]  for r in auction_rows]
            total_qty = sum(qtys)
            avg   = round(sum(p*q for p,q in zip(prices,qtys)) / total_qty) if total_qty else 0
            sorted_p = sorted(zip(prices,qtys), reverse=True)
            top_n = max(1, len(sorted_p)//2)
            top50_rows = sorted_p[:top_n]
            top50_qty = sum(q for _,q in top50_rows)
            top50_avg = round(sum(p*q for p,q in top50_rows)/top50_qty) if top50_qty else 0
            daily_report.append({
                "품종": variety, "규격": spec,
                "평균가": avg, "상위50평균가": top50_avg,
                "최고가": max(prices), "최저가": min(prices),
                "총거래량": int(total_qty),
                "총중량": round(total_qty * float(spec.replace("kg","").strip() or 0) if "kg" in spec else 0, 1),
            })
    return {"auction": {"daily_report": daily_report}}


def make_trend_snapshot(data_list, group_key, date_str):
    from collections import defaultdict
    groups = defaultdict(list)
    for r in data_list:
        groups[r.get(group_key,"")].append(r)
    snapshots = []
    for variety, rows in groups.items():
        auction_rows = remove_outliers([r for r in rows if r.get("is_auction")])
        if not auction_rows:
            continue
        prices = [r["경락가"] for r in auction_rows]
        qtys   = [r["거래량"]  for r in auction_rows]
        total_qty = sum(qtys)
        avg = round(sum(p*q for p,q in zip(prices,qtys))/total_qty) if total_qty else 0
        sorted_p = sorted(zip(prices,qtys), reverse=True)
        top_n = max(1, len(sorted_p)//2)
        top50_rows = sorted_p[:top_n]
        top50_qty = sum(q for _,q in top50_rows)
        top50_avg = round(sum(p*q for p,q in top50_rows)/top50_qty) if top50_qty else 0
        snapshots.append({"품종": variety, "날짜": date_str, "평균가": avg, "상위50평균가": top50_avg, "거래량": int(total_qty)})
    return snapshots


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


# ── 메인 ─────────────────────────────────────────────────────

print(f"=== {PATCH_DATE} 데이터 패치 시작 ===")

raw = fetch_all_pages(PATCH_DATE)
print(f"총 {len(raw)}건 수집")

mangam_data, gamgyul_data, hobak_data = classify(raw)
print(f"만감류 {len(mangam_data)}건 / 감귤 {len(gamgyul_data)}건 / 호박 {len(hobak_data)}건")

mangam_stats  = make_stats(mangam_data,  "품종")
gamgyul_stats = make_stats(gamgyul_data, "품종")
hobak_stats   = make_stats(hobak_data,   "품목")

mangam_snap  = make_trend_snapshot(mangam_data,  "품종", PATCH_DATE)
gamgyul_snap = make_trend_snapshot(gamgyul_data, "품종", PATCH_DATE)
hobak_snap   = make_trend_snapshot(hobak_data,   "품목", PATCH_DATE)

trend = load_trend("trend.json")
before = sorted(trend.get("mangam", {}).keys())
print(f"패치 전 날짜: {before}")

trend = update_trend(trend, "mangam",  mangam_snap,  PATCH_DATE, mangam_stats["auction"]["daily_report"])
trend = update_trend(trend, "gamgyul", gamgyul_snap, PATCH_DATE, gamgyul_stats["auction"]["daily_report"])
trend = update_trend(trend, "hobak",   hobak_snap,   PATCH_DATE, hobak_stats["auction"]["daily_report"])

save_trend("trend.json", trend)
after = sorted(trend.get("mangam", {}).keys())
print(f"패치 후 날짜: {after}")
print(f"=== 패치 완료: {PATCH_DATE} 추가됨 ===")
