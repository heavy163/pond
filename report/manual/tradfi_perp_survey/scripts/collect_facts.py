"""采集《跨所 TradFi 永续排查》报告的全部数字，输出 data/facts.json。

数字全部来自实测：
  - 币安 /fapi/v1/exchangeInfo 的 underlyingType（交易所权威分类）
  - ClickHouse cryptos.instrument_class / instrument（我们自己对 8 家 + upbit 的分类结果）

用法:
  /usr/bin/python3.12 report/manual/tradfi_perp_survey/scripts/collect_facts.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
import httpx

HERE = Path(__file__).resolve()
OUT = HERE.parents[1] / "data" / "facts.json"
CH = dict(host="192.168.1.200", port=8123, username="default", password="")
DB = "cryptos"
BINANCE = "https://fapi.binance.com/fapi/v1/exchangeInfo"

# 报告里要重点讲的撞车案例
COLLISION_CASES = ["DIA", "SPX", "ALL", "META"]
# 跨所对照用的典型标的
CROSS_CASES = ["AAPL", "NVDA", "TSLA", "XAU", "SPX500", "OPENAI"]


def main() -> None:
    # ── 1) 币安：权威分类 ─────────────────────────────────────
    j = httpx.get(BINANCE, timeout=30).json()
    syms = j["symbols"]
    ut = Counter((s.get("underlyingType") or "NONE") for s in syms)
    ct = Counter((s.get("contractType") or "NONE") for s in syms)
    tradfi = {}
    coins = set()
    for s in syms:
        t = (s.get("underlyingType") or "").upper()
        base = s["baseAsset"].upper()
        if t == "COIN":
            coins.add(base)
        elif t:
            tradfi[base] = {
                "underlying_type": t.lower(),
                "symbol": s["symbol"],
                "contract_type": s.get("contractType"),
            }
    collision_cases = {b: tradfi.get(b) or {"underlying_type": "coin", "symbol": f"{b}USDT"}
                       for b in COLLISION_CASES}

    # ── 2) 我们自己的分类结果 ────────────────────────────────
    c = clickhouse_connect.get_client(**CH)
    rows = c.query(f"""
        SELECT venue, code, asset_class, underlying, matched_via
        FROM {DB}.instrument_class FINAL
    """).result_rows
    types = dict(c.query(f"""
        SELECT code, any(contract_type) FROM {DB}.instrument FINAL GROUP BY code
    """).result_rows)
    # instrument_class 里没有 contract_type，用 instrument 表补；同一 code 在现货/合约都可能出现，
    # 所以按 (venue, code) 从 instrument 取全部类型
    itypes = defaultdict(set)
    for v, code, t in c.query(f"""
        SELECT venue, code, contract_type FROM {DB}.instrument FINAL
    """).result_rows:
        itypes[(v, code)].add(t)

    agg = defaultdict(Counter)          # (venue, mkt) -> asset_class count
    by_cat = defaultdict(Counter)       # venue -> category count（仅 perp）
    examples = defaultdict(list)        # (venue, mkt, class) -> codes
    ambiguous = []
    for venue, code, cls, und, via in rows:
        for mkt in sorted(itypes.get((venue, code), {"perp"})):
            agg[(venue, mkt)][cls] += 1
            if mkt == "perp" and cls not in ("crypto", "ambiguous"):
                by_cat[venue][cls] += 1
            if cls != "crypto" and len(examples[(venue, mkt, cls)]) < 4:
                examples[(venue, mkt, cls)].append(code)
            if cls == "ambiguous":
                ambiguous.append({"venue": venue, "code": code, "underlying": und,
                                  "matched_via": via, "market": mkt})

    venues = sorted({v for v, _ in agg}, key=lambda x: (x == "upbit", x))
    by_venue_type = []
    for v, mkt in sorted(agg, key=lambda k: (k[0], k[1])):
        cc = agg[(v, mkt)]
        total = sum(cc.values())
        eq = cc["equity"] + cc["hk_equity"] + cc["kr_equity"] + cc["cn_equity"] + cc["premarket"]
        tf = eq + cc["commodity"] + cc["index"] + cc["fx"]
        by_venue_type.append({
            "venue": v, "market": mkt, "total": total,
            "crypto": cc["crypto"], "equity": cc["equity"],
            "hk_equity": cc["hk_equity"], "kr_equity": cc["kr_equity"],
            "cn_equity": cc["cn_equity"], "premarket": cc["premarket"],
            "commodity": cc["commodity"], "index": cc["index"], "fx": cc["fx"],
            "ambiguous": cc["ambiguous"],
            "tradfi": tf, "tradfi_pct": round(tf / total * 100, 1) if total else 0.0,
        })

    # ── 3) 跨所同一标的的清单 ────────────────────────────────
    cross = {}
    for base in CROSS_CASES:
        hit = {}
        for v, code, cls, und, via in rows:
            if und.upper() == base and cls != "crypto" and (v, code) in itypes:
                hit.setdefault(v, code)
        cross[base] = hit

    # ── 4) 分类依据分布（matched_via）─────────────────────────
    via_counts = Counter(r[4] for r in rows)

    facts = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "binance": {
            "total_symbols": len(syms),
            "contract_type": dict(ct),
            "underlying_type": dict(ut),
            "tradifi_underlyings": len(tradfi),
            "tradifi_breakdown": dict(Counter(v["underlying_type"] for v in tradfi.values())),
            "collision_cases": collision_cases,
        },
        "ours": {
            "instrument_class_rows": len(rows),
            "matched_via": dict(via_counts),
            "by_venue_type": by_venue_type,
            "perp_category": {v: dict(c2) for v, c2 in by_cat.items()},
            "ambiguous": ambiguous,
            "cross_venue": cross,
        },
        "examples": {f"{v}/{mkt}/{cls}": codes
                     for (v, mkt, cls), codes in examples.items()},
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(facts, ensure_ascii=False, indent=2), encoding="utf-8")
    c.close()
    print(f"[facts] {OUT}")
    print(f"  币安合约 {len(syms)}，TradFi 标的 {len(tradfi)}")
    print(f"  我们分类 {len(rows)} 条，所×类型 {len(by_venue_type)} 组")
    print(f"  撞车待判 {len(ambiguous)} 条")


if __name__ == "__main__":
    main()
