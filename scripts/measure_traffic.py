# -*- coding: utf-8 -*-
"""测量每个交易所每一轮同步实际下载多少字节。

做法：每个所取 3 个合约，按真实窗口 [top-6h, top) 调 kline / funding / oi，
读适配器新加的 stats["bytes"]，再乘以该所的合约数，得到每轮流量估算。

不写库（不碰 ClickHouse），只发少量请求（每所 ~10 次），流量可忽略。
"""
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path("/mnt/hgfs/e/workspace/FutureTradeDev/pond")))

from pond.cryptos.config import CryptosConfig
from pond.cryptos.venues.registry import make_adapter

LOOKBACK = 6
N_PROBE = 3


def fmt(n):
    if n >= 1 << 30:
        return f"{n / (1 << 30):.2f} GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.1f} MB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.1f} KB"
    return f"{n} B"


async def probe(spec, start, end):
    """返回 dict：各 dtype 的单合约字节/行数，以及该所的合约数。"""
    out = {"venue": spec.name, "codes": 0, "spot_codes": 0, "detail": {}, "errors": []}
    async with make_adapter(spec) as ad:
        try:
            inst = await ad.instruments()
        except Exception as e:
            out["errors"].append(f"instruments: {type(e).__name__}: {e}")
            return out
        out["codes"] = len(inst)
        raws = [i["raw_code"] for i in inst[:N_PROBE] if i.get("raw_code")]
        if not raws:
            return out

        for dtype, fn in (
            ("kline", lambda r: ad.kline(r, start, end, market="perp")),
            ("funding", lambda r: ad.funding(r, start, end)),
            ("oi", lambda r: ad.open_interest(r, start, end)),
        ):
            # 预热：bybit/hyperliquid 的 OI 首次调用会顺带拉全量 ticker（~200 KB），
            # 不预热就会把这笔一次性开销摊到 3 个探针上，算出「单合约 227 KB」的假数。
            try:
                await fn(raws[0])
            except Exception:
                pass
            b0, r0 = ad.stats["bytes"], ad.stats["requests"]
            nrows = 0
            for r in raws:
                try:
                    nrows += len(await fn(r))
                except Exception as e:
                    out["errors"].append(f"{dtype} {r}: {type(e).__name__}: {str(e)[:60]}")
            b1, r1 = ad.stats["bytes"], ad.stats["requests"]
            out["detail"][dtype] = {
                "bytes": (b1 - b0) / len(raws),
                "reqs": (r1 - r0) / len(raws),
                "rows": nrows / len(raws),
            }

        # 现货
        try:
            spots = await ad.spot_instruments()
        except Exception:
            spots = []
        out["spot_codes"] = len(spots)
        if spots:
            try:
                await ad.kline(spots[0]["raw_code"], start, end, market="spot")
            except Exception:
                pass
            b0 = ad.stats["bytes"]
            nrows = 0
            for s in spots[:N_PROBE]:
                try:
                    nrows += len(await ad.kline(s["raw_code"], start, end, market="spot"))
                except Exception as e:
                    out["errors"].append(f"spot: {type(e).__name__}: {str(e)[:50]}")
            out["detail"]["spot"] = {
                "bytes": (ad.stats["bytes"] - b0) / max(1, min(N_PROBE, len(spots))),
                "reqs": 1.0, "rows": nrows / max(1, min(N_PROBE, len(spots))),
            }
        out["stats"] = dict(ad.stats)
    return out


async def main():
    cfg = CryptosConfig.load(None)
    top = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start, end = top - timedelta(hours=LOOKBACK), top
    print(f"窗口 [{start:%Y-%m-%d %H:%M}, {end:%Y-%m-%d %H:%M}) UTC，每所探 {N_PROBE} 个合约\n")

    specs = cfg.enabled_venues()
    results = await asyncio.gather(*[probe(s, start, end) for s in specs])

    print(f"{'venue':<12}{'codes':>7}{'spot':>6} | " +
          " | ".join(f"{d:>22}" for d in ("kline", "funding", "oi", "spot")) + " |     每轮估算")
    print("-" * 132)
    total = 0
    rows_total = 0
    for r in results:
        per_round = 0
        cells = []
        for d in ("kline", "funding", "oi", "spot"):
            v = r["detail"].get(d)
            if not v:
                cells.append(f"{'-':>22}")
                continue
            n_codes = r["spot_codes"] if d == "spot" else r["codes"]
            est = v["bytes"] * n_codes
            per_round += est
            rows_total += v["rows"] * n_codes
            cells.append(f"{fmt(v['bytes']):>8}/{v['rows']:.1f}行".rjust(22))
        total += per_round
        print(f"{r['venue']:<12}{r['codes']:>7}{r['spot_codes']:>6} | " + " | ".join(cells)
              + f" | {fmt(per_round):>10}")
        for e in r["errors"][:2]:
            print(f"    ! {e}")

    print("-" * 132)
    print(f"合计每轮 ≈ {fmt(total)}   下载行数 ≈ {rows_total:,.0f}   放大比 ≈ {total / max(1, rows_total) / 1000:.0f} KB/行")
    print(f"按每小时一轮 → 每天 {fmt(total * 24)}，每月 {fmt(total * 24 * 30)}")


if __name__ == "__main__":
    asyncio.run(main())
