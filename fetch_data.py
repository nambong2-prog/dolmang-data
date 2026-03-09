import requests
import json
import os
from datetime import datetime

API_KEY = os.environ.get("API_KEY")
today = datetime.now().strftime("%Y-%m-%d")

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

MANGAM_VARIETIES = ["레드향", "천혜향", "한라봉", "카라향"]
HOBAK_ITEMS = ["미니밤호박", "단호박"]
JEJU_ORIGINS = ["제주"]
TARGET_LCLSF = ["06"]

# 누적 데이터 보관 일수
HISTORY_DAYS = 60


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
                    print(f"  대분류 {lclsf_cd}: 데이터 없음")
                    break
                item_list = items.get("item", [])
                if isinstance(item_list, dict):
                    item_list = [item_list]
                all_items.extend(item_list)
                print(f"  대분류 {lclsf_cd} / 페이지 {page}: {len(item_list)}건 (누적 {len(all_items)}, 전체 {total})")
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
    trd_se = (item.get("trd_se") or "").strip()
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
        "단위수량": unit_qty,          # float으로 저장 (총중량 계산용)
        "경락가": int(float(item.get("scsbd_prc") or 0)),
        "거래량": int(float(item.get("qty") or 0)),
        "낙찰일시": (item.get("scsbd_dt") or ""),
        "매매방법": trd_se,
        "is_auction": "정가수의" not in trd_se,
    }


def filter_items(all_items):
    mangam_data, gamgyul_data, hobak_data = [], [], []
    for item in all_items:
        vrty_nm = (item.get("corp_gds_vrty_nm") or "").strip()
        item_nm = (item.get("corp_gds_item_nm") or "").strip()
        plor_nm = (item.get("plor_nm") or "").strip()
        mclsf_cd = (item.get("gds_mclsf_cd") or "").strip()

        if mclsf_cd == "15" and vrty_nm in MANGAM_VARIETIES:
            mangam_data.append(parse_item(item, "만감류", "품종"))
        elif mclsf_cd != "15" and "감귤" in item_nm:
            gamgyul_data.append(parse_item(item, "감귤", "품종"))
        elif any(h in item_nm for h in HOBAK_ITEMS):
            if any(j in plor_nm for j in JEJU_ORIGINS):
                hobak_data.append(parse_item(item, "호박", "품목"))
    return mangam_data, gamgyul_data, hobak_data


def remove_outliers(records):
    """
    IQR 방식으로 이상값 제거.
    경락가 기준 중앙값의 0.3배 ~ 3.5배 범위 밖은 제거.
    건수가 4건 미만이면 그대로 반환 (샘플 부족).
    """
    if len(records) < 4:
        return records
    prices = sorted(r["price"] for r in records)
    n = len(prices)
    median = prices[n // 2] if n % 2 == 1 else (prices[n//2-1] + prices[n//2]) / 2
    lo = median * 0.3
    hi = median * 3.5
    filtered = [r for r in records if lo <= r["price"] <= hi]
    # 필터 후 너무 많이 줄면 원본 반환 (이상값이 아닐 수 있음)
    if len(filtered) < max(1, len(records) * 0.5):
        return records
    removed = len(records) - len(filtered)
    if removed > 0:
        print(f"    이상값 {removed}건 제거 (기준: {int(lo):,}~{int(hi):,}원, 중앙값 {int(median):,}원)")
    return filtered


def make_stats(data_list, group_key="품종"):
    auction = [r for r in data_list if r.get("is_auction")]
    jungga  = [r for r in data_list if not r.get("is_auction")]

    def calc(rows):
        daily_report  = {}
        market_detail = {}

        for row in rows:
            f  = row[group_key]
            m  = row["도매시장"]
            s  = row["규격"]
            p  = row["경락가"]
            q  = row["거래량"]
            uq = float(row.get("단위수량") or 0)   # 박스당 kg

            # ── 일일리포트 (품종+규격별) ──
            dk = f"{f}_{s}"
            if dk not in daily_report:
                daily_report[dk] = {
                    "품종": f, "규격": s,
                    "총거래량": 0, "총중량": 0.0, "총액": 0,
                    "최고가": 0, "최저가": 999999,
                    "records": []
                }
            daily_report[dk]["총거래량"] += q
            daily_report[dk]["총중량"]   += q * uq
            daily_report[dk]["총액"]     += p * q
            daily_report[dk]["records"].append({"price": p, "qty": q, "market": m})
            if p > daily_report[dk]["최고가"]: daily_report[dk]["최고가"] = p
            if p < daily_report[dk]["최저가"]: daily_report[dk]["최저가"] = p

            # ── 시장별 ──
            corp = row.get("법인") or ""
            mk = f"{f}_{s}_{m}"
            if mk not in market_detail:
                market_detail[mk] = {"품종": f, "규격": s, "시장명": m,
                                     "총거래량": 0, "총액": 0, "최고가": 0,
                                     "records": [], "법인별": {}}
            market_detail[mk]["총거래량"] += q
            market_detail[mk]["총액"]     += p * q
            market_detail[mk]["records"].append({"price": p, "qty": q})
            if p > market_detail[mk]["최고가"]: market_detail[mk]["최고가"] = p
            if corp:
                if corp not in market_detail[mk]["법인별"]:
                    market_detail[mk]["법인별"][corp] = {"법인": corp, "총거래량": 0, "총액": 0, "최고가": 0, "records": []}
                market_detail[mk]["법인별"][corp]["총거래량"] += q
                market_detail[mk]["법인별"][corp]["총액"]     += p * q
                market_detail[mk]["법인별"][corp]["records"].append({"price": p, "qty": q})
                if p > market_detail[mk]["법인별"][corp]["최고가"]: market_detail[mk]["법인별"][corp]["최고가"] = p

        # ── 일일리포트 마무리 ──
        daily_list = []
        for v in daily_report.values():
            if v["총거래량"] > 0:
                records = v.pop("records")
                v["평균가"]   = round(v["총액"] / v["총거래량"])
                v["총중량"]   = round(v["총중량"], 1)   # 소수점 1자리

                # ── 이상값 제거 ──
                clean_records = remove_outliers(records)

                # ── 이상값 제거 후 평균가·최고가·최저가 재계산 ──
                if clean_records:
                    clean_qty = sum(r["qty"] for r in clean_records)
                    v["평균가"]  = round(sum(r["price"]*r["qty"] for r in clean_records) / clean_qty)
                    v["최고가"]  = max(r["price"] for r in clean_records)
                    v["최저가"]  = min(r["price"] for r in clean_records)
                    v["총거래량"] = clean_qty
                    v["총중량"]  = round(sum(
                        r["qty"] * float(next((row.get("단위수량",0) for row in rows
                            if row["경락가"]==r["price"]), 0) or 0)
                        for r in clean_records), 1)

                # ── 상위 50% (경락가 기준 건수 상위 50%) ──
                sorted_clean = sorted(clean_records, key=lambda x: -x["price"])
                n50 = max(1, len(sorted_clean) // 2)     # 건수 상위 50%
                top50_records = sorted_clean[:n50]

                if top50_records:
                    t50_qty = sum(r["qty"] for r in top50_records)
                    v["상위50평균가"] = round(sum(r["price"]*r["qty"] for r in top50_records) / t50_qty)
                    v["상위50기준가"] = min(r["price"] for r in top50_records)  # 상위50% 중 최저가 = 진입 기준선
                    v["상위50거래량"] = t50_qty
                else:
                    v["상위50평균가"] = v["상위50기준가"] = v["상위50거래량"] = 0
                daily_list.append(v)

        # ── 시장별 TOP6 (상위50% 평균가 기준) ──
        market_list = []
        for v in market_detail.values():
            if v["총거래량"] > 0:
                v["평균가"] = round(v["총액"] / v["총거래량"])
                # 상위50% 평균가 계산
                recs = v["records"]
                sorted_recs = sorted(recs, key=lambda x: -x["price"])
                n50 = max(1, len(sorted_recs) // 2)
                top50 = sorted_recs[:n50]
                t50_qty = sum(r["qty"] for r in top50)
                v["상위50평균가"] = round(sum(r["price"]*r["qty"] for r in top50) / t50_qty) if t50_qty else v["평균가"]
                # 법인별 집계 마무리 (상위50% 평균가 계산)
                corp_list = []
                for cd in v["법인별"].values():
                    cd["평균가"] = round(cd["총액"] / cd["총거래량"]) if cd["총거래량"] else 0
                    cd_recs = cd["records"]
                    cd_sorted = sorted(cd_recs, key=lambda x: -x["price"])
                    cd_n50 = max(1, len(cd_sorted) // 2)
                    cd_top50 = cd_sorted[:cd_n50]
                    cd_t50_qty = sum(r["qty"] for r in cd_top50)
                    cd["상위50평균가"] = round(sum(r["price"]*r["qty"] for r in cd_top50) / cd_t50_qty) if cd_t50_qty else cd["평균가"]
                    del cd["records"]
                    corp_list.append(cd)
                corp_list.sort(key=lambda x: -x["상위50평균가"])
                v["법인목록"] = corp_list
                del v["records"], v["법인별"]
                market_list.append(v)

        spec_market_top6 = {}
        for v in market_list:
            key = f"{v['품종']}_{v['규격']}"
            if key not in spec_market_top6:
                spec_market_top6[key] = {"품종": v["품종"], "규격": v["규격"], "시장목록": []}
            spec_market_top6[key]["시장목록"].append({
                "시장명": v["시장명"], "평균가": v["평균가"], "상위50평균가": v["상위50평균가"],
                "최고가": v["최고가"], "거래량": v["총거래량"], "법인목록": v["법인목록"]
            })
        for key in spec_market_top6:
            spec_market_top6[key]["시장목록"].sort(key=lambda x: -x["상위50평균가"])
            spec_market_top6[key]["시장목록"] = spec_market_top6[key]["시장목록"][:6]

        # ── 이상값 제거된 전체내역 별도 구성 ──
        clean_rows = []
        # 품종+규격별로 중앙값 계산 후 이상값 row 제거
        group_records = {}
        for row in rows:
            key = f"{row[group_key]}_{row['규격']}"
            if key not in group_records:
                group_records[key] = []
            group_records[key].append(row)

        for key, group in group_records.items():
            recs = [{"price": r["경락가"], "qty": r["거래량"], "_row": r} for r in group]
            clean = remove_outliers(recs)
            clean_rows.extend([r["_row"] for r in clean])

        return {
            "daily_report": daily_list,
            "market_top6":  list(spec_market_top6.values()),
            "total_history": clean_rows
        }

    return {"auction": calc(auction), "jungga": calc(jungga)}


def make_trend_snapshot(data_list, group_key, date_str):
    """
    오늘 날짜의 품종+규격별 평균가·거래량 요약 (가격추이용)
    auction 데이터만 사용
    """
    auction = [r for r in data_list if r.get("is_auction")]
    summary = {}
    for row in auction:
        f  = row[group_key]
        s  = row["규격"]
        p  = row["경락가"]
        q  = row["거래량"]
        uq = float(row.get("단위수량") or 0)
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
                "날짜":     date_str,
                "품종":     v["품종"],
                "규격":     v["규격"],
                "평균가":   round(v["총액"] / v["총거래량"]),
                "총거래량": v["총거래량"],
                "총중량":   round(v["총중량"], 1),
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
    """
    trend[cat][날짜] = [스냅샷 목록]
    trend[cat+'_daily'][날짜] = daily_report (일일리포트 날짜별 보관)
    최대 HISTORY_DAYS일치만 보관
    """
    if cat not in trend:
        trend[cat] = {}
    trend[cat][date_str] = snapshots

    # 날짜별 daily_report 저장
    if daily_report is not None:
        dkey = cat + "_daily"
        if dkey not in trend:
            trend[dkey] = {}
        trend[dkey][date_str] = daily_report
        dates_d = sorted(trend[dkey].keys())
        if len(dates_d) > HISTORY_DAYS:
            for old in dates_d[:-HISTORY_DAYS]:
                del trend[dkey][old]

    # 오래된 날짜 제거
    dates = sorted(trend[cat].keys())
    if len(dates) > HISTORY_DAYS:
        for old in dates[:-HISTORY_DAYS]:
            del trend[cat][old]
    return trend


# ============================================================
# 메인 실행
# ============================================================
print(f"API 데이터 수집 시작: {today}")
all_items = fetch_all_pages(today)
print(f"감귤/만감류 수집 완료: {len(all_items)}건")

mangam_data, gamgyul_data, hobak_data = filter_items(all_items)
print(f"만감류: {len(mangam_data)}건 / 감귤: {len(gamgyul_data)}건 / 제주호박: {len(hobak_data)}건")

# 데이터 없을 때 샘플
if not mangam_data and not gamgyul_data and not hobak_data:
    print("실제 데이터 없음 - 샘플 데이터 사용")
    mangam_data = [
        {"카테고리":"만감류","품종":"천혜향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":3.0,"경락가":26000,"거래량":120,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"만감류","품종":"천혜향","품목":"감귤","도매시장":"부산반여","법인":"동부청과","원산지":"제주특별자치도","규격":"5kg","단위":"kg","단위수량":5.0,"경락가":38000,"거래량":80,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"만감류","품종":"레드향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":3.0,"경락가":32000,"거래량":90,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"만감류","품종":"레드향","품목":"감귤","도매시장":"구리","법인":"구리청과","원산지":"제주특별자치도","규격":"5kg","단위":"kg","단위수량":5.0,"경락가":45000,"거래량":40,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"만감류","품종":"한라봉","품목":"감귤","도매시장":"서울가락","법인":"한국청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":3.0,"경락가":22000,"거래량":60,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"만감류","품종":"천혜향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":3.0,"경락가":24000,"거래량":100,"낙찰일시":"","매매방법":"정가수의(예약형)","is_auction":False},
    ]
    gamgyul_data = [
        {"카테고리":"감귤","품종":"온주밀감","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도","규격":"5kg","단위":"kg","단위수량":5.0,"경락가":18000,"거래량":100,"낙찰일시":"","매매방법":"경매","is_auction":True},
    ]
    hobak_data = []

# ── 오늘 통계 생성 ──
mangam_stats = make_stats(mangam_data, "품종")
gamgyul_stats = make_stats(gamgyul_data, "품종")
hobak_stats   = make_stats(hobak_data, "품목")

# ── 가격추이 스냅샷 ──
mangam_snap = make_trend_snapshot(mangam_data, "품종", today)
gamgyul_snap = make_trend_snapshot(gamgyul_data, "품종", today)
hobak_snap   = make_trend_snapshot(hobak_data, "품목", today)

# ── 누적 trend.json 업데이트 ──
trend = load_trend("trend.json")
trend = update_trend(trend, "mangam",  mangam_snap,  today, mangam_stats["auction"]["daily_report"])
trend = update_trend(trend, "gamgyul", gamgyul_snap, today, gamgyul_stats["auction"]["daily_report"])
trend = update_trend(trend, "hobak",   hobak_snap,   today, hobak_stats["auction"]["daily_report"])
save_trend("trend.json", trend)
print(f"trend.json 저장: mangam {len(trend['mangam'])}일치")

# ── data.json 저장 ──
output = {
    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "mangam":  mangam_stats,
    "gamgyul": gamgyul_stats,
    "hobak":   hobak_stats,
}
with open("data.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print("data.json 저장 완료")
