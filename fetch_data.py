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
MANGAM_VARIETIES = ["레드향", "천혜향", "한라봉", "카라향"]
HOBAK_ITEMS = ["미니밤호박", "단호박"]
JEJU_ORIGINS = ["제주"]
TARGET_LCLSF = ["06"]
# ============================================================


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

    # 규격: 단위수량(kg) 기반으로 표준화
    try:
        kg = float(unit_qty)
        if kg > 0:
            spec = f"{int(kg)}kg" if kg == int(kg) else f"{kg}kg"
        else:
            spec = (item.get("pkg_nm") or "")
    except:
        spec = (item.get("pkg_nm") or "")

    return {
        "카테고리": category,
        "품종": vrty_nm if group_key == "품종" else (item.get("corp_gds_item_nm") or "").strip(),
        "품목": item_nm,
        "도매시장": (item.get("whsl_mrkt_nm") or ""),
        "법인": (item.get("corp_nm") or ""),
        "원산지": plor_nm,
        "규격": spec,
        "단위": (item.get("unit_nm") or ""),
        "단위수량": str(unit_qty),
        "경락가": int(float(item.get("scsbd_prc") or 0)),
        "거래량": int(float(item.get("qty") or 0)),
        "낙찰일시": (item.get("scsbd_dt") or ""),
        "매매방법": trd_se,
        "is_auction": "정가수의" not in trd_se,  # 경매 여부
    }


def filter_items(all_items):
    mangam_data = []
    gamgyul_data = []
    hobak_data = []

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


def make_stats(data_list, group_key="품종"):
    """
    경매/정가수의 분리 후 통계 생성
    - auction: 경매 데이터만
    - jungga: 정가수의 데이터만
    각각 daily_report, top6, market_top6, total_history 포함
    """

    auction = [r for r in data_list if r.get("is_auction")]
    jungga = [r for r in data_list if not r.get("is_auction")]

    def calc(rows):
        top6 = {}
        daily_report = {}
        market_detail = {}

        for row in rows:
            f = row[group_key]
            m = row["도매시장"]
            s = row["규격"]
            p = row["경락가"]
            q = row["거래량"]

            # TOP6 (20박스 이상)
            if q >= 20:
                if f not in top6:
                    top6[f] = []
                top6[f].append(row)

            # 일일 리포트 (품종+규격별)
            dk = f"{f}_{s}"
            if dk not in daily_report:
                daily_report[dk] = {
                    "품종": f, "규격": s,
                    "총거래량": 0, "총액": 0,
                    "최고가": 0, "최저가": 999999,
                    "records": []
                }
            daily_report[dk]["총거래량"] += q
            daily_report[dk]["총액"] += p * q
            daily_report[dk]["records"].append({"price": p, "qty": q, "market": m})
            if p > daily_report[dk]["최고가"]:
                daily_report[dk]["최고가"] = p
            if p < daily_report[dk]["최저가"]:
                daily_report[dk]["최저가"] = p

            # 시장별
            mk = f"{f}_{s}_{m}"
            if mk not in market_detail:
                market_detail[mk] = {
                    "품종": f, "규격": s, "시장명": m,
                    "총거래량": 0, "총액": 0, "최고가": 0
                }
            market_detail[mk]["총거래량"] += q
            market_detail[mk]["총액"] += p * q
            if p > market_detail[mk]["최고가"]:
                market_detail[mk]["최고가"] = p

        # TOP6 정렬
        for k in top6:
            top6[k].sort(key=lambda x: x["경락가"], reverse=True)
            top6[k] = top6[k][:6]

        # 일일 리포트 계산
        daily_list = []
        for v in daily_report.values():
            if v["총거래량"] > 0:
                records = v.pop("records")
                v["평균가"] = round(v["총액"] / v["총거래량"])

                # 상위 50% 계산 (거래량 기준 상위 50%)
                sorted_r = sorted(records, key=lambda x: -x["price"])
                total_qty = sum(r["qty"] for r in sorted_r)
                cum = 0
                top50_records = []
                for r in sorted_r:
                    top50_records.append(r)
                    cum += r["qty"]
                    if cum >= total_qty * 0.5:
                        break

                if top50_records:
                    top50_qty = sum(r["qty"] for r in top50_records)
                    top50_avg = round(sum(r["price"] * r["qty"] for r in top50_records) / top50_qty)
                    top50_min = min(r["price"] for r in top50_records)
                else:
                    top50_avg = 0
                    top50_min = 0
                    top50_qty = 0

                v["상위50평균가"] = top50_avg
                v["상위50기준가"] = top50_min
                v["상위50거래량"] = top50_qty
                daily_list.append(v)

        # 시장별 TOP6 (규격별 평균가 기준)
        market_list = []
        for v in market_detail.values():
            if v["총거래량"] > 0:
                v["평균가"] = round(v["총액"] / v["총거래량"])
                market_list.append(v)

        # 규격별 시장 TOP6
        spec_market_top6 = {}
        for v in market_list:
            key = f"{v['품종']}_{v['규격']}"
            if key not in spec_market_top6:
                spec_market_top6[key] = {
                    "품종": v["품종"],
                    "규격": v["규격"],
                    "시장목록": []
                }
            spec_market_top6[key]["시장목록"].append({
                "시장명": v["시장명"],
                "평균가": v["평균가"],
                "최고가": v["최고가"],
                "거래량": v["총거래량"]
            })

        for key in spec_market_top6:
            spec_market_top6[key]["시장목록"].sort(key=lambda x: -x["평균가"])
            spec_market_top6[key]["시장목록"] = spec_market_top6[key]["시장목록"][:6]

        return {
            "top6": top6,
            "daily_report": daily_list,
            "market_top6": list(spec_market_top6.values()),
            "total_history": rows
        }

    return {
        "auction": calc(auction),
        "jungga": calc(jungga)
    }


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

# 데이터 없을 때 샘플
if not mangam_data and not gamgyul_data and not hobak_data:
    print("실제 데이터 없음 - 샘플 데이터 사용")
    mangam_data = [
        {"카테고리":"만감류","품종":"천혜향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":"3.0","경락가":24000,"거래량":150,"낙찰일시":"","매매방법":"정가수의(예약형)","is_auction":False},
        {"카테고리":"만감류","품종":"천혜향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":"3.0","경락가":26000,"거래량":100,"낙찰일시":"","매매방법":"정가수의(예약형)","is_auction":False},
        {"카테고리":"만감류","품종":"레드향","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":"3.0","경락가":27000,"거래량":290,"낙찰일시":"","매매방법":"정가수의(예약형)","is_auction":False},
        {"카테고리":"만감류","품종":"레드향","품목":"감귤","도매시장":"부산엄궁","법인":"항도청과","원산지":"제주특별자치도","규격":"3kg","단위":"kg","단위수량":"3.0","경락가":32000,"거래량":80,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"만감류","품종":"한라봉","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"3kg","단위":"kg","단위수량":"3.0","경락가":23000,"거래량":140,"낙찰일시":"","매매방법":"정가수의(예약형)","is_auction":False},
        {"카테고리":"만감류","품종":"한라봉","품목":"감귤","도매시장":"구리","법인":"구리청과","원산지":"제주특별자치도","규격":"3kg","단위":"kg","단위수량":"3.0","경락가":35000,"거래량":50,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"만감류","품종":"카라향","품목":"감귤","도매시장":"대전노은","법인":"대전청과","원산지":"제주특별자치도","규격":"3kg","단위":"kg","단위수량":"3.0","경락가":30000,"거래량":30,"낙찰일시":"","매매방법":"경매","is_auction":True},
    ]
    gamgyul_data = [
        {"카테고리":"감귤","품종":"온주밀감","품목":"감귤","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도","규격":"5kg","단위":"kg","단위수량":"5.0","경락가":18000,"거래량":100,"낙찰일시":"","매매방법":"경매","is_auction":True},
        {"카테고리":"감귤","품종":"온주밀감","품목":"감귤","도매시장":"부산엄궁","법인":"항도청과","원산지":"제주특별자치도","규격":"5kg","단위":"kg","단위수량":"5.0","경락가":15000,"거래량":80,"낙찰일시":"","매매방법":"경매","is_auction":True},
    ]
    hobak_data = [
        {"카테고리":"호박","품종":"단호박","품목":"단호박","도매시장":"서울가락","법인":"서울청과","원산지":"제주특별자치도 서귀포시","규격":"8kg","단위":"kg","단위수량":"8.0","경락가":25000,"거래량":40,"낙찰일시":"","매매방법":"경매","is_auction":True},
    ]

mangam_stats = make_stats(mangam_data, "품종")
gamgyul_stats = make_stats(gamgyul_data, "품종")
hobak_stats = make_stats(hobak_data, "품목")

output = {
    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "mangam": mangam_stats,
    "gamgyul": gamgyul_stats,
    "hobak": hobak_stats
}

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print("data.json 저장 완료")
