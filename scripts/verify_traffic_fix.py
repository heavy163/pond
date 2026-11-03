# -*- coding: utf-8 -*-
"""等价性验证：改了 page_limit 之后，同一窗口拉回来的行数必须和库里已存的一致。

挑一个【已经过去、库里肯定有】的 6 小时窗口，用适配器重新拉，和 ClickHouse 里
同窗口的行数逐合约对比。行数少一根都说明改动把数据截断了。
"""
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path("/mnt/hgfs/e/workspace/FutureTradeDev/pond")))

import clickhouse_connect
from pond.cryptos.config import CryptosConfig
from pond.cryptos.venues.registry import make_adapter

N = 6          # 每所抽几个合约
HOURS = 6      # 窗口长度（与生产一致）
# 取一个已经落库的过去窗口：整点对齐、再往前退 3 天
TOP = (datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
       - timedelta(days=3))
START, END = TOP - timedelta(hours=HOURS), TOP

TABLES = {
    "kline": ("cryptos.kline_futures_1h", "open_time"),
    "spot": ("cryptos.kline_spot_1h", "open_time"),
    "funding": ("cryptos.funding_rate", "datetime"),
    "oi": ("cryptos.open_interest", "datetime"),
}


async def check(spec, ch):
    res = {"venue": spec.name, "rows": {}}
    async with make_adapter(spec) as ad:
        inst = await ad.instruments()
        raws = [i["raw_code"] for i in inst[:N]]
        try:
            spots = [s["raw_code"] for s in (await ad.spot_instruments())[:N]]
        except Exception:
            spots = []

        async def one(dtype, raw, fn, table, tcol):
            try:
                got = len(await fn(raw))
            except Exception as e:
                return (dtype, raw, -1, -1, f"{type(e).__name__}")
            code = ad.resolve_code(raw)
            q = (f"SELECT count() FROM {table} WHERE venue='{spec.name}' AND code='{code}' "
                 f"AND {tcol} >= toDateTime({int(START.timestamp())}) "
                 f"AND {tcol} < toDateTime({int(END.timestamp())})")
            have = int(ch.query(q).result_rows[0][0])
            return (dtype, raw, got, have, "")

        jobs = []
        for r in raws:
            jobs.append(one("kline", r, lambda x: ad.kline(x, START, END, market="perp"),
                            *TABLES["kline"]))
            jobs.append(one("funding", r, lambda x: ad.funding(x, START, END), *TABLES["funding"]))
            jobs.append(one("oi", r, lambda x: ad.open_interest(x, START, END), *TABLES["oi"]))
        for r in spots:
            jobs.append(one("spot", r, lambda x: ad.kline(x, START, END, market="spot"),
                            *TABLES["spot"]))
        for dtype, raw, got, have, err in await asyncio.gather(*jobs):
            res["rows"].setdefault(dtype, []).append((raw, got, have, err))
    return res


async def main():
    ch = clickhouse_connect.get_client(host="192.168.1.200", port=8123,
                                       username="default", password="")
    cfg = CryptosConfig.load(None)
    print(f"窗口 [{START:%Y-%m-%d %H:%M}, {END:%Y-%m-%d %H:%M}) UTC，每所抽 {N} 个合约\n")
    out = await asyncio.gather(*[check(s, ch) for s in cfg.enabled_venues()])

    bad = 0
    for r in out:
        line = []
        for d in ("kline", "spot", "funding", "oi"):
            items = r["rows"].get(d)
            if not items:
                continue
            got = sum(x[1] for x in items if x[1] >= 0)
            have = sum(x[2] for x in items)
            lost = [x for x in items if x[1] >= 0 and x[1] < x[2]]
            errs = [x for x in items if x[1] < 0]
            flag = "✓" if not lost else f"✗丢{len(lost)}"
            bad += len(lost)
            line.append(f"{d}: 拉回{got}/库{have} {flag}" + (f" 险{len(errs)}" if errs else ""))
        print(f"{r['venue']:<12} " + " | ".join(line))
        for d, items in r["rows"].items():
            for raw, got, have, err in items:
                if got >= 0 and got < have:
                    print(f"    ✗ {d} {raw}: 拉回 {got} < 库 {have}")
    print(f"\n{'全部一致' if bad == 0 else f'有 {bad} 处少于库内数据，必须回退'}")

    # 抽 8h 整点窗口验证 funding（资金费率 8h 结算，6h 窗口可能天生为 0）
    fs, fe = START - timedelta(hours=8), END
    print(f"\n[补验] funding 用 14 小时窗口 [{fs:%m-%d %H:%M}, {fe:%m-%d %H:%M})：")
    for spec in cfg.enabled_venues()[:4]:
        async with make_adapter(spec) as ad:
            inst = await ad.instruments()
            for i in inst[:3]:
                raw = i["raw_code"]
                try:
                    got = len(await ad.funding(raw, fs, fe))
                except Exception as e:
                    print(f"  {spec.name} {raw}: {type(e).__name__}"); continue
                have = int(ch.query(
                    f"SELECT count() FROM cryptos.funding_rate WHERE venue='{spec.name}' "
                    f"AND code='{ad.resolve_code(raw)}' "
                    f"AND datetime >= toDateTime({int(fs.timestamp())}) "
                    f"AND datetime < toDateTime({int(fe.timestamp())})").result_rows[0][0])
                mark = "✓" if got >= have else "✗"
                print(f"  {mark} {spec.name:<12} {raw:<14} 拉回 {got} / 库 {have}")


if __name__ == "__main__":
    asyncio.run(main())
