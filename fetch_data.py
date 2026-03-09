import requests
import json
import os
from datetime import datetime

API_KEY = os.environ.get("API_KEY")
today = datetime.now().strftime("%Y-%m-%d")

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

# ============================================================
# 수집 대상 설정
# ============================================================
# 만감류 품종명 (corp_gds_vrty_nm 기준)
MANGAM_VARIETIES = ["레드향", "천혜향", "한라봉", "카라향"]

# 호박 품목명 (corp_gds_item_nm 기준) + 원산지 제주 필터
HOBAK_ITEMS = ["미니밤호박", "단호박"]
JEJU_ORIGINS = ["제주"]

# 대분류 코드
# 06 = 감귤/만감류
# 호박은 여름 시즌에 코드 확인 후 추가 예정
TARGET_LCLSF = ["06"]
# ============================================================


def fetch_all_pages(date_str):
    """대분류 코드로 필터해서 수집"""
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
                print(f"  API 오류 (대분류 {lclsf_cd} / 페이지 {page}): {e}")
                break

    return all_items


def filter_items(all_items):
    """수집 대상 필터링"""
    mangam_data = []
    gamgyul_data = []
    hobak_data = []

    for item in all_items:
        vrty_nm = (item.get("corp_gds_vrty_nm") or "").strip()
        item_nm = (item.get("corp_gds_item_nm") or "").strip()
        plor_nm = (item.get("plor_nm") or "").strip()
        mclsf_cd = (item.get("gds_mclsf_cd") or "").strip()

        # 1. 만감류 (중분류 15 = 만감 + 품종명 매칭)
        if mclsf_cd == "15" and vrty_nm in MANGAM_VARIETIES:
            mangam_data.append({
                "카테고리": "만감류",
                "품종": vrty_nm,
                "품목": item_nm,
                "도매시장": (item.get("whsl_mrkt_nm") or ""),
                "법인": (item.get("corp_nm") or ""),
                "원산지": plor_nm,
                "규격": (item.get("pkg_nm") or ""),
                "단위": (item.get("unit_nm") or ""),
                "단위수량": (item.get("unit_qty") or ""),
                "경락가": int(float(item.get("scsbd_prc") or 0)),
                "거래량": int(float(item.get("qty") or 0)),
                "낙찰일시": (item.get("scsbd_dt") or ""),
                "매매방법": (item.get("trd_se") or ""),
            })

        # 2. 일반 감귤 (대분류 06 + 중분류 15 아닌 것)
        elif mclsf_cd != "15" and "감귤" in item_nm:
            gamgyul_data.append({
                "카테고리": "감귤",
                "품종": vrty_nm,
                "품목": item_nm,
                "도매시장": (item.get("whsl_mrkt_nm") or ""),
                "법인": (item.get("corp_nm") or ""),
                "원산지": plor_nm,
                "규격": (item.get("pkg_nm") or ""),
                "단위": (item.get("unit_nm") or ""),
                "단위수량": (item.get("unit_qty") or ""),
                "경락가": int(float(item.get("scsbd_prc") or 0)),
                "거래량": int(float(item.get("qty") or 0)),
                "낙찰일시": (item.get("scsbd_dt") or ""),
                "매매방법": (item.get("trd_se") or ""),
            })

        # 3. 호박 (여름 시즌용 - 원산지 제주 필터)
        elif any(h in item_nm for h in HOBAK_ITEMS):
            if any(j in plor_nm for j in JEJU_ORIGINS):
                hobak_data.append({
                    "카테고리": "호박",
                    "품종": vrty_nm,
                    "품목": item_nm,
                    "도매시장": (item.get("whsl_mrkt_nm") or ""),
                    "법인": (item.get("corp_nm") or ""),
                    "원산지": plor_nm,
                    "규격": (item.get("pkg_nm") or ""),
                    "단위": (item.get("unit_nm") or ""),
                    "단위수량": (item.get("unit_qty") or ""),
                    "경락가": int(float(item.get("scsbd_prc") or 0)),
                    "거래량": int(float(item.get("qty") or 0)),
                    "낙찰일시": (item.get("scsbd_dt") or ""),
                    "매매방법": (item.get("trd_se") or ""),
                })

    return mangam_data, gamgyul_data, hobak_data


def make_stats(data_list, group_key="품종"):
    """품종/규격별 통계 생성"""
    top6 = {}
    daily_report = {}
    market_detail = {}

    for row in data_list:
        f = row[group_key]
        m = row["도매시장"]
        s = row["규격"]
        p = row["경락가"]
        q = row["거래량"]

        if q >= 20:
            if f not in top6:
                top6[f] = []
            top6[f].append(row)

        dk = f"{f}_{s}"
        if dk not in daily_report:
            daily_report[dk] = {
                "품종": f, "규격": s,
                "총거래량": 0, "총액": 0,
                "최고가": 0, "최저가": 999999
            }
        daily_report[dk]["총거래량"] += q
        daily_report[dk]["총액"] += p * q
        if p > daily_report[dk]["최고가"]:
            daily_report[dk]["최고가"] = p
        if p < daily_report[dk]["최저가"]:
            daily_report[dk]["최저가"] = p

        mk = f"{f}_{m}"
        if mk not in market_detail:
            market_detail[mk] = {
                "품종": f, "시장명": m,
                "총거래량": 0, "총액": 0, "최고가": 0
            }
        market_detail[mk]["총거래량"] += q
        market_detail[mk]["총액"] += p * q
        if p > market_detail[mk]["최고가"]:
            market_detail[mk]["최고가"] = p

    for k in top6:
        top6[k].sort(key=lambda x: x["경락가"], reverse=True)
        top6[k] = top6[k][:6]

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

    return top6, daily_list, market_list


# ============================================================
# 메인 실행
# ============================================================
print(f"API 데이터 수집 시작: {today}")
all_items = fetch_all_pages(today)
print(f"감귤/만감류 수집 완료: {len(all_items)}건")

mangam_data, gamgyul_data, hobak_data = filter_items(all_items)

print(f"만감류: {len(mangam_data)}건")
print(f"감귤: {len(gamgyul_data)}건")
print(f"제주 호박: {len(hobak_data)}건")

# 통계 생성
mangam_top6, mangam_daily, mangam_market = make_stats(mangam_data, "품종")
gamgyul_top6, gamgyul_daily, gamgyul_market = make_stats(gamgyul_data, "품종")
hobak_top6, hobak_daily, hobak_market = make_stats(hobak_data, "품목")

# 데이터 없을 때 샘플
if not mangam_data and not gamgyul_data and not hobak_data:
    print("실제 데이터 없음 - 샘플 데이터 사용")
    mangam_data = [
        {"카테고리":"만감류","품종":"천혜향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":"3","경락가":24000,"거래량":150,"낙찰일시":"","매매방법":"정가수의"},
        {"카테고리":"만감류","품종":"레드향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":"3","경락가":27000,"거래량":290,"낙찰일시":"","매매방법":"정가수의"},
        {"카테고리":"만감류","품종":"한라봉","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":"3","경락가":23000,"거래량":140,"낙찰일시":"","매매방법":"정가수의"},
        {"카테고리":"만감류","품종":"카라향","품목":"감귤","도매시장":"부산엄궁","법인":"항도청과","원산지":"제주특별자치도","규격":"3kg","단위":"kg","단위수량":"3","경락가":32000,"거래량":20,"낙찰일시":"","매매방법":"경매"},
    ]
    gamgyul_data = [
        {"카테고리":"감귤","품종":"온주밀감","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도","규격":"5kg","단위":"kg","단위수량":"5","경락가":18000,"거래량":100,"낙찰일시":"","매매방법":"경매"},
    ]
    hobak_data = [
        {"카테고리":"호박","품종":"단호박","품목":"단호박","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"8kg","단위":"kg","단위수량":"8","경락가":25000,"거래량":40,"낙찰일시":"","매매방법":"경매"},
    ]
    mangam_top6, mangam_daily, mangam_market = make_stats(mangam_data, "품종")
    gamgyul_top6, gamgyul_daily, gamgyul_market = make_stats(gamgyul_data, "품종")
    hobak_top6, hobak_daily, hobak_market = make_stats(hobak_data, "품목")

output = {
    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "mangam": {
        "top6": mangam_top6,
        "daily_report": mangam_daily,
        "market_detail": mangam_market,
        "total_history": mangam_data
    },
    "gamgyul": {
        "top6": gamgyul_top6,
        "daily_report": gamgyul_daily,
        "market_detail": gamgyul_market,
        "total_history": gamgyul_data
    },
    "hobak": {
        "top6": hobak_top6,
        "daily_report": hobak_daily,
        "market_detail": hobak_market,
        "total_history": hobak_data
    }
}

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print("data.json 저장 완료")
