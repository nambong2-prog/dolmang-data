# -*- coding: utf-8 -*-
"""
build_v2data.py
스크래퍼 결과(data.json)의 그날치 원자료를 v2 데이터센터 누적본(v2-data.json)에 병합한다.
- fetch_data.py 실행 직후, 같은 워크플로우에서 실행된다 (data.json 이 repo 루트에 있음).
- v2.html 의 categoryOf 규칙을 그대로 미러링한다: 제주 단호박/미니밤호박만, 감귤/만감류 키워드.
- 수동 업로드한 날짜(src=manual)는 절대 덮어쓰지 않는다.
- 기존 날짜/데이터는 그대로 보존한다. data.json 이 이상하면 아무것도 쓰지 않고 종료(데이터 보호).
"""
import json, os, sys, datetime

DATA_FILE = "data.json"
V2_FILE   = "v2-data.json"

CATS = ["hobak", "gamgyul", "mangam"]
MANGAM_KW = ["천혜향", "한라봉", "레드향", "황금향", "카라향", "진지향", "천백향"]
GAMGYUL_KW = ["감귤", "노지", "하우스감귤", "타이벡"]
HOBAK_VARIETIES = ["단호박", "미니밤호박"]
JEJU_ONLY = {"hobak": True, "gamgyul": False, "mangam": False}


def category_of(row):
    pum = str(row.get("품목", "") or "")
    jong = str(row.get("품종", "") or "")
    origin = str(row.get("원산지", "") or "")
    if any(k in jong or k in pum for k in MANGAM_KW):
        return "mangam"
    if any(k in jong or k in pum for k in GAMGYUL_KW):
        return "gamgyul"
    if "호박" in pum or "호박" in jong:
        if "수입" in jong:
            return None
        if not any(jong.startswith(k) for k in HOBAK_VARIETIES):
            return None
        if "제주" in origin:
            return "hobak"
        return None if JEJU_ONLY["hobak"] else "hobak"
    return None


def num(v):
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0


def to_int(v):
    n = num(v)
    return int(n) if n == int(n) else n


def is_jungga(row):
    m = str(row.get("매매방법", "") or "")
    return ("정가" in m) or ("수의" in m)


def pull_rows(node):
    """auction + jungga 의 total_history 원자료 행을 모두 모은다."""
    out = []
    if not isinstance(node, dict):
        return out
    for k in ("auction", "jungga"):
        sub = node.get(k) or {}
        for r in (sub.get("total_history") or []):
            out.append(r)
    return out


def v2_row(r):
    return {
        "품목": str(r.get("품목", "") or ""),
        "품종": str(r.get("품종", "") or ""),
        "도매시장": str(r.get("도매시장", "") or ""),
        "법인": str(r.get("법인", "") or ""),
        "원산지": str(r.get("원산지", "") or ""),
        "규격": str(r.get("규격", "") or ""),
        "경락가": to_int(r.get("경락가", 0)),
        "거래량": to_int(r.get("거래량", 0)),
        "낙찰일시": str(r.get("낙찰일시", "") or ""),
    }


def main():
    # 1) data.json 로드 (실패하면 안전하게 종료 — v2-data.json 손대지 않음)
    if not os.path.exists(DATA_FILE):
        print("[build_v2data] data.json 없음 → 종료")
        return 0
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print("[build_v2data] data.json 파싱 실패 → 종료(보호):", e)
        return 0

    ut = str(data.get("update_time", "") or "")
    ut_date = ut[:10] if len(ut) >= 10 else datetime.date.today().isoformat()

    # 2) 스크래퍼 원자료 → (cat, date) 별로 분류
    #    day_rows[cat][date] = {"total":[...], "jtotal":[...]}
    day_rows = {c: {} for c in CATS}
    src_map = {"hobak": data.get("hobak"), "gamgyul": data.get("gamgyul"), "mangam": data.get("mangam")}
    for cat, node in src_map.items():
        for r in pull_rows(node):
            cc = category_of(r)  # 제주/품종/키워드 필터로 최종 판정 (노드 카테고리보다 우선)
            if cc is None:
                continue
            # 도매시장 "거래일"은 전날 오후~당일 새벽에 걸침. 스크래퍼는 trd_clcln_ymd=오늘로 조회하므로
            # 모든 행을 거래일(ut_date)로 묶는다 (수동 엑셀의 거래일자 기준과 일치). 낙찰일시로 쪼개지 않음.
            d = ut_date
            bucket = day_rows[cc].setdefault(d, {"total": [], "jtotal": []})
            (bucket["jtotal"] if is_jungga(r) else bucket["total"]).append(v2_row(r))

    total_new = sum(len(b["total"]) + len(b["jtotal"]) for c in CATS for b in day_rows[c].values())
    if total_new == 0:
        print("[build_v2data] 신규 원자료 0건(주말/휴장 등) → v2-data.json 변경 없음")
        return 0

    # 3) 기존 v2-data.json 로드 (있는데 파싱 실패하면 절대 덮어쓰지 않음)
    base = {"version": 2, "updated": "", "cats": {c: {"days": {}} for c in CATS}}
    if os.path.exists(V2_FILE):
        try:
            with open(V2_FILE, encoding="utf-8") as f:
                base = json.load(f)
        except Exception as e:
            print("[build_v2data] 기존 v2-data.json 파싱 실패 → 종료(보호):", e)
            return 0
    base.setdefault("version", 2)
    base.setdefault("cats", {})
    for c in CATS:
        base["cats"].setdefault(c, {"days": {}})
        base["cats"][c].setdefault("days", {})

    # ── 일회성 정리: 날짜 분류 버그로 2026-07-05에 잘못 들어간 auto 데이터 제거 ──
    for c in CATS:
        e = base["cats"][c]["days"].get("2026-07-05")
        if isinstance(e, dict) and e.get("src") == "auto":
            del base["cats"][c]["days"]["2026-07-05"]
            print("[build_v2data] 정리: %s 2026-07-05 (오분류 auto) 제거" % c)

    # 4) 병합: 수동(src=manual) 날짜는 건너뜀, 나머지는 auto 스냅샷으로 갱신
    changed = 0
    skipped_manual = 0
    for cat in CATS:
        days = base["cats"][cat]["days"]
        for d, bucket in day_rows[cat].items():
            if not bucket["total"] and not bucket["jtotal"]:
                continue
            existing = days.get(d)
            if isinstance(existing, dict):
                cur_src = existing.get("src", "manual")  # src 없으면 legacy 수동으로 간주 → 보호
                if cur_src == "manual":
                    skipped_manual += 1
                    continue
            days[d] = {"total": bucket["total"], "jtotal": bucket["jtotal"], "src": "auto"}
            changed += 1

    if changed == 0:
        print("[build_v2data] 갱신 대상 없음(수동보호 %d일) → 변경 없음" % skipped_manual)
        return 0

    base["updated"] = datetime.datetime.utcnow().isoformat() + "Z"

    with open(V2_FILE, "w", encoding="utf-8") as f:
        json.dump(base, f, ensure_ascii=False, separators=(",", ":"))

    print("[build_v2data] 완료: %d일 갱신(auto), 수동보호 %d일, 기준 %s" % (changed, skipped_manual, ut_date))
    for cat in CATS:
        for d, bucket in sorted(day_rows[cat].items()):
            n = len(bucket["total"]) + len(bucket["jtotal"])
            if n:
                print("   - %s %s: %d행" % (cat, d, n))
    return 0


if __name__ == "__main__":
    sys.exit(main())
