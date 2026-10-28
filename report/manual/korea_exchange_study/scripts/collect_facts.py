"""采集《韩国交易所研究》全部事实 → data/facts.json。

原则：notebook 里出现的每个数字都必须来自这里，notebook 只做断言与绘图，
不手抄、不推断。第三方统计单独放 third_party 并标注「未独立复核」。

用法: /usr/bin/python3.12 report/manual/korea_exchange_study/scripts/collect_facts.py
"""

from __future__ import annotations

import datetime as dt
import json
import statistics
import subprocess
from pathlib import Path

import httpx

H = {"User-Agent": "pond-cryptos/0.1", "Accept": "application/json"}
CH = "http://192.168.1.200:8123/"
OUT = Path(__file__).resolve().parents[1] / "data" / "facts.json"
OUT.parent.mkdir(parents=True, exist_ok=True)


def ch(sql: str) -> list[list[str]]:
    r = subprocess.run(
        ["curl", "-s", "--max-time", "60", CH, "--data-binary", sql + " FORMAT TSV"],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr[:300])
    return [ln.split("\t") for ln in r.stdout.strip().split("\n") if ln]


def get(url: str, **kw):
    t = dt.datetime.now()
    r = httpx.get(url, headers=H, timeout=25, **kw)
    ms = int((dt.datetime.now() - t).total_seconds() * 1000)
    return r, ms


# ─────────────── 1. 五家交易所体量 ───────────────
def exchanges() -> dict:
    out: dict = {}

    # Upbit（KRW 市场逐个取 24h 成交额，分块）
    try:
        mk = get("https://api.upbit.com/v1/market/all")[0].json()
        krw = [m["market"] for m in mk if m["market"].startswith("KRW-")]
        tot = 0.0
        for i in range(0, len(krw), 90):
            d = get(f"https://api.upbit.com/v1/ticker?markets={','.join(krw[i:i+90])}")[0].json()
            tot += sum(float(x["acc_trade_price_24h"]) for x in d)
        out["Upbit"] = {"markets": len(krw), "vol24_krw": tot, "status": "ok"}
    except Exception as e:
        out["Upbit"] = {"status": f"error: {type(e).__name__}"}

    # Bithumb
    try:
        d = get("https://api.bithumb.com/public/ticker/ALL_KRW")[0].json()
        data = d["data"]
        rows = {k: v for k, v in data.items() if k != "date"}
        out["Bithumb"] = {
            "markets": len(rows),
            "vol24_krw": sum(float(v["acc_trade_value_24H"]) for v in rows.values()),
            "status": "ok",
        }
    except Exception as e:
        out["Bithumb"] = {"status": f"error: {type(e).__name__}"}

    # Coinone
    try:
        d = get("https://api.coinone.co.kr/public/v2/ticker_new/KRW")[0].json()
        tk = d["tickers"]
        out["Coinone"] = {
            "markets": len(tk),
            "vol24_krw": sum(float(x.get("quote_volume") or 0) for x in tk),
            "status": "ok",
        }
    except Exception as e:
        out["Coinone"] = {"status": f"error: {type(e).__name__}"}

    # Gopax
    try:
        d = get("https://api.gopax.co.kr/tickers")[0].json()
        krw = [x for x in d if x["tradingPairName"].endswith("-KRW")]
        out["Gopax"] = {
            "markets": len(krw),
            "vol24_krw": sum(float(x.get("quoteVolume") or 0) for x in krw),
            "status": "ok",
        }
    except Exception as e:
        out["Gopax"] = {"status": f"error: {type(e).__name__}"}

    # digitalX（原 Korbit）：公共接口部分不可用，如实记录
    try:
        r, ms = get("https://api.digitalx.miraeasset.com/v1/tradingPairs")
        out["digitalX(原Korbit)"] = {
            "markets": None,
            "vol24_krw": None,
            "status": "partial" if r.status_code >= 400 else "ok",
            "http": r.status_code,
            "note": "tradingPairs 接口不可用；仅逐对 ticker 可取",
        }
    except Exception as e:
        out["digitalX(原Korbit)"] = {"status": f"error: {type(e).__name__}"}
    return out


# ─────────────── 2. NIL 上市日实测 ───────────────
def nil_case() -> dict:
    r = get("https://api.upbit.com/v1/candles/minutes/1",
            params={"market": "KRW-NIL", "to": "2026-10-27 10:00:00", "count": 200})[0]
    up_raw = r.json()
    if isinstance(up_raw, dict):
        return {"status": "error", "detail": up_raw}
    up = sorted(up_raw, key=lambda x: x["candle_date_time_utc"])

    T0 = int(dt.datetime(2026, 10, 27, 7, 40, tzinfo=dt.timezone.utc).timestamp() * 1000)
    by_raw = get("https://api.bybit.com/v5/market/kline",
                 params={"category": "linear", "symbol": "NILUSDT", "interval": "1",
                         "start": T0, "end": T0 + 7200_000})[0].json()
    by = {}
    for x in by_raw["result"]["list"]:
        ts = dt.datetime.fromtimestamp(int(x[0]) / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M")
        by[ts] = float(x[4])

    fx = float(get("https://api.upbit.com/v1/ticker", params={"markets": "KRW-USDT"})[0]
               .json()[0]["trade_price"])

    up = {x["candle_date_time_utc"][:16]: x for x in up}
    common = sorted(set(up) & set(by))
    series = []
    for i, t in enumerate(common):
        krw = float(up[t]["trade_price"])
        b = by[t]
        series.append({
            "i": i, "t": t, "krw": krw, "usd": krw / fx, "bybit": b,
            "prem": krw / fx / b - 1,
        })

    h1 = [up[t] for t in common[:60]]
    open_px = float(sorted(up_raw, key=lambda x: x["candle_date_time_utc"])[0]["opening_price"])
    h1_max = max(float(x["high_price"]) for x in h1)
    return {
        "status": "ok",
        "market": "KRW-NIL",
        "fx": fx,
        "first_candle_utc": common[0],
        "open": open_px,
        "usd_open": open_px / fx,
        "h1_max": h1_max,
        "h1_ret_from_open": h1_max / open_px - 1,
        "h1_quote_krw": sum(float(x["candle_acc_trade_price"]) for x in h1),
        "first_min_quote_krw": float(sorted(up_raw, key=lambda x: x["candle_date_time_utc"])[0]["candle_acc_trade_price"]),
        "bybit_first": by[common[0]],
        "prem_at_first": series[0]["prem"],
        "prem_min10": next(s["prem"] for s in series if s["i"] >= 9),
        "prem_min60": next(s["prem"] for s in series if s["i"] >= 59),
        "n_minutes": len(common),
        "close_last": series[-1]["usd"],
        "ret_last_from_open": series[-1]["usd"] / (open_px / fx) - 1,
        "series": series,
    }


# ─────────────── 3. 管道落地实测 ───────────────
def pipeline() -> dict:
    venues = ch("SELECT uniqExact(venue) FROM cryptos.sync_stat WHERE window_end >= now() - INTERVAL 1 HOUR")[0][0]
    up = ch("SELECT count(), uniqExact(code), min(open_time), max(open_time) FROM cryptos.kline_spot_1h WHERE venue='upbit'")[0]
    fx = ch("SELECT rate, toString(datetime) FROM cryptos.fx_rate ORDER BY datetime DESC LIMIT 1")[0]
    prem_rows = ch("""
WITH (SELECT rate FROM cryptos.fx_rate ORDER BY datetime DESC LIMIT 1) AS fx,
     (SELECT max(open_time) FROM cryptos.kline_spot_1h WHERE venue='upbit') AS t
SELECT u.base, round((u.close/fx)/b.close - 1, 6)
FROM (SELECT splitByChar('-',code)[1] AS base, close FROM cryptos.kline_spot_1h
      WHERE venue='upbit' AND open_time=t) u
JOIN (SELECT splitByChar('-',code)[1] AS base, close FROM cryptos.kline_futures_1h
      WHERE venue='bybit' AND open_time=t) b ON b.base=u.base
ORDER BY 2 DESC
""")
    prem = [(r[0], float(r[1])) for r in prem_rows]
    vals = [p for _, p in prem]
    return {
        "n_venues_active": int(venues),
        "upbit_spot_rows": int(up[0]),
        "upbit_codes": int(up[1]),
        "upbit_first": up[2],
        "upbit_last": up[3],
        "fx_rate": float(fx[0]),
        "fx_at": fx[1],
        "premium": prem,
        "premium_n": len(prem),
        "premium_mean": statistics.fmean(vals),
        "premium_median": statistics.median(vals),
        "premium_max": max(vals),
        "premium_min": min(vals),
        "premium_btc": next((p for c, p in prem if c == "BTC"), None),
    }


facts = {
    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    "exchanges": exchanges(),
    "nil": nil_case(),
    "pipeline": pipeline(),
    # 第三方统计：未独立复核，仅作背景，notebook 中明确标注来源与样本量
    "third_party": {
        "source": "getcoinsight / 4pillars（网页统计，未独立复核）",
        "listing_24h_median_pct": {
            "Upbit": 7.2, "Bybit": 1.9, "Binance": 1.3, "OKX": -0.3, "Bithumb": -8.4,
        },
        "listing_24h_n": {"Upbit": 233, "Bybit": 222, "Binance": 114, "OKX": 152, "Bithumb": 8},
        "upbit_first_day_median_pct_2026h1": 42.0,
        "upbit_first_day_median_pct_after_20260615": 10.0,
        "upbit_vol_feb_usd_m": 1900,
        "upbit_vol_aug_usd_m": 460,
        "korbit_to_digitalx": {
            "acquired_pct": 97.15,
            "legal_rename": "2026-08-11",
            "service_switch": "2026-09-16",
            "new_domain": "digitalx.miraeasset.com",
        },
    },
}

OUT.write_text(json.dumps(facts, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"[facts] {OUT}")
print(f"  交易所: { {k: v.get('markets') for k, v in facts['exchanges'].items()} }")
print(f"  NIL 首分钟溢价: {facts['nil'].get('prem_at_first')}")
print(f"  管道: {facts['pipeline']['n_venues_active']} 所 / upbit {facts['pipeline']['upbit_codes']} 码 "
      f"/ premium n={facts['pipeline']['premium_n']} median={facts['pipeline']['premium_median']:.4%}")
