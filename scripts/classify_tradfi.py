"""给每个合约打上「标的资产类别」标签，用来把加密标的和传统金融标的（TradFi）分开。

为什么要做这件事
----------------
mexc/gate/bitget/okx 都有 20%~36% 的永续合约，标的其实是美股 / 港股 / 韩股 / 商品 / 指数
（AAPL、NVDA、TSLA、CSX、ALAB、XAU、SPX500、NAS100、US30…）。
它们和币安 2026 年上线的 TradFi 永续是同一类东西。
做加密横截面研究会**被这些标的污染**：它们周末不动、成交时段受股票开盘限制、
和 BTC 的相关性接近 0，混进因子池会直接毁掉 IC。

判断依据（不靠猜）
------------------
币安的 `/fapi/v1/exchangeInfo` 里每个合约带 `underlyingType` 字段：
COIN / EQUITY / HK_EQUITY / KR_EQUITY / CN_EQUITY / COMMODITY / INDEX / PREMARKET / FX，
`contractType=TRADIFI_PERPETUAL` 就是 TradFi 合约。这是**交易所自己给的权威分类**，
比按名字猜靠谱。其他所没有这个字段，所以用「基准资产名对齐币安」来判定。

用法:
  /usr/bin/python3.12 scripts/classify_tradfi.py            # 打印分类结果
  /usr/bin/python3.12 scripts/classify_tradfi.py --write    # 同时写 cryptos.instrument_class
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import clickhouse_connect
import httpx

CH = dict(host="192.168.1.200", port=8123, username="default", password="")
DB = "cryptos"
BINANCE = "https://fapi.binance.com/fapi/v1/exchangeInfo"

DDL = f"""
CREATE TABLE IF NOT EXISTS {DB}.instrument_class (
    venue        LowCardinality(String),
    code         LowCardinality(String),
    asset_class  LowCardinality(String),   -- crypto / equity / hk_equity / kr_equity /
                                           -- cn_equity / commodity / index / premarket / fx
    underlying   String,                   -- 对齐到的标的代码（币安 baseAsset 或本地推断）
    matched_via  LowCardinality(String),   -- binance / heuristic / default
    updated_at   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (venue, code)
"""

# 币安没有、但各所确实在挂的传统标的别名（人工核对过的）
# 只放「名字本身就是唯一含义」的别名。带 500/100/US30 后缀的指数名不会和币撞车；
# 裸 SPX / DIA 会（SPX6900 meme 币、DIA 预言机币），所以不放这里，留给 COLLISIONS。
ALIASES: dict[str, tuple[str, str]] = {
    "XAU": ("commodity", "XAU"), "XAG": ("commodity", "XAG"),
    "XAUT": ("commodity", "XAU"), "PAXG": ("commodity", "XAU"),
    "SPX500": ("index", "SPX"), "US500": ("index", "SPX"),
    "NDX100": ("index", "NDX"), "NAS100": ("index", "NDX"),
    "US30": ("index", "DJI"),
}

# 代号撞车：同一个 ticker 既是币又是传统标的，光看名字判不出来。
# 例：币安 DIAUSDT 是 DIA 预言机币（underlyingType=COIN），不是道指 ETF DIA；
#     SPXUSDT 是 SPX6900 meme 币，不是标普 500；ALLUSDT 币安标的是 INDEX。
# 这类一律标 ambiguous，等成交量指纹（周末/夜间零成交）判定。
COLLISIONS = {"DIA", "SPX", "ALL", "MEDIATEK", "MSTR", "COIN", "MARA", "RIOT", "META"}

# 纯加密现货所：整所只有币，没有传统标的。命名撞车要按所剔除，
# 例如 upbit 的 META-KRW 是 Metadium（韩国币），不是 Meta Platforms；
# SPX-KRW 是 SPX6900 meme 币，不是标普 500。
CRYPTO_ONLY_VENUES = {"upbit"}


def normalize(base: str) -> str:
    b = base.upper()
    b = re.sub(r"^PRE", "", b)        # pre-IPO 前缀
    b = re.sub(r"STOCK$", "", b)      # gate/mexc 的 STOCK 后缀
    b = re.sub(r"(100|500)$", "", b)  # NDX100 / SPX500
    return b


def fetch_binance() -> tuple[dict[str, str], set[str]]:
    """返回 (TradFi 映射, 币安认定为币的 ticker 集合)。"""
    j = httpx.get(BINANCE, timeout=25).json()
    tradfi: dict[str, str] = {}
    coins: set[str] = set()
    for s in j["symbols"]:
        ut = (s.get("underlyingType") or "").upper()
        base = s["baseAsset"].upper()
        if ut == "COIN":
            coins.add(base)
        elif ut:
            tradfi[base] = ut.lower()
    return tradfi, coins


def classify(base: str, tradfi: dict[str, str], coins: set[str]) -> tuple[str, str, str]:
    """返回 (asset_class, underlying, matched_via)

    判定优先级（越靠前越可信）：
      1. 交易所自己的命名：STOCK 后缀 / pre- 前缀（gate/mexc/bitget 显式标记）
      2. 币安 TradFi 映射（underlyingType 是交易所给的权威字段）
      3. 撞车 ticker → ambiguous（必须靠成交量指纹再判）
      4. 人工别名表
      5. 默认 crypto
    """
    n = normalize(base)
    b = base.upper()

    if b.endswith("STOCK") or n.endswith("STOCK"):     # 1. 显式后缀
        return "equity", n, "venue_suffix"
    if b.startswith("PRE"):                            # 1. pre-IPO
        return "premarket", b[3:], "venue_suffix"

    if b in ALIASES:                                   # 2. 无歧义别名（SPX500/NAS100/XAUT…）
        cls, und = ALIASES[b]
        return cls, und, "heuristic"
    if n in tradfi and n not in COLLISIONS:            # 3. 币安权威映射
        if n in coins:                                # 币安自己既当币又当传统标的
            return "ambiguous", n, "collision"
        return tradfi[n], n, "binance"
    if n in COLLISIONS and (n in coins or n in tradfi):  # 4. 撞车
        return "ambiguous", n, "collision"
    return "crypto", b, "default"



def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="写 cryptos.instrument_class")
    args = ap.parse_args()

    tradfi, coins = fetch_binance()
    print(f"币安 TradFi 标的 {len(tradfi)} 个：{dict(Counter(tradfi.values()))}\n")

    c = clickhouse_connect.get_client(**CH)
    rows = c.query(f"""
        SELECT venue, code, contract_type FROM {DB}.instrument FINAL
        WHERE contract_type IN ('perp', 'spot')
    """).result_rows

    now = datetime.now(timezone.utc)
    out = []
    per_venue: dict[str, Counter] = defaultdict(Counter)
    samples: dict[tuple[str, str], list[str]] = defaultdict(list)
    for venue, code, ctype in rows:
        cls, und, via = classify(code.split("-")[0], tradfi, coins)
        if venue in CRYPTO_ONLY_VENUES:
            cls, via = "crypto", "venue_crypto_only"
        per_venue[f"{venue}/{ctype}"][cls] += 1
        if cls != "crypto" and len(samples[(f"{venue}/{ctype}", cls)]) < 3:
            samples[(f"{venue}/{ctype}", cls)].append(code)
        out.append([venue, code, cls, und, via, now])

    print(f"{'venue':<13}{'合约数':>7}{'加密':>7}{'股票类':>8}{'商品':>6}{'指数':>6}{'待判':>9}{'TradFi%':>9}")
    print("-" * 66)
    for v in sorted(per_venue):
        cc = per_venue[v]
        eq = cc["equity"] + cc["hk_equity"] + cc["kr_equity"] + cc["cn_equity"] + cc["premarket"]
        cm, idx = cc["commodity"], cc["index"]
        total = sum(cc.values())
        tf = eq + cm + idx + cc["fx"]
        print(f"{v:<13}{total:>7}{cc['crypto']:>7}{eq:>8}{cm:>6}{idx:>6}"
              f"{cc['ambiguous']:>9}{tf / total * 100:>10.1f}%")

    print("\n样例：")
    for (v, cls), codes in sorted(samples.items()):
        print(f"  {v}/{cls}: {codes}")

    if args.write:
        c.command(DDL)
        c.insert(f"{DB}.instrument_class", out,
                 column_names=["venue", "code", "asset_class", "underlying", "matched_via", "updated_at"])
        print(f"\n已写入 {DB}.instrument_class：{len(out)} 行")
    else:
        print("\n（dry-run：加 --write 才写 instrument_class）")

    c.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
