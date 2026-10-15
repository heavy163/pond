"""国内商品期货【逐合约】日线回填（→ quant.cn_futures_contract_1d）。

为什么需要这张表
----------------
quant.cn_futures_kline_1d 是【主力连续】（新浪 futures_main_sina），它在换月处是
**价格拼接**，会产生假跳空。实测全表 3455 天（2.1%）是假跳空，这些天平均 |收益|
476bp（正常日 ~100bp）：RB0 在 2015-06-24 单日跳空 +980bp，而当日高低区间只有 76bp。
拿它直接算收益 ⇒ 波动率、回撤、趋势信号全错 ⇒ 所有期货研究结论不可信。
本脚本回填逐合约数据，供 research 侧重建【无跳空】的连续收益序列。

数据源（重要：不统一）
--------------------
- SHFE / CZCE / INE / GFEX：akshare.get_futures_daily(market=...) 交易所逐日全合约。
- DCE：**交易所接口在本机不可用** —— www.dce.com.cn 返回 412（WAF 拦截）、
  portal.dce.com.cn DNS/连接不通、浏览器 ERR_BLOCKED_BY_CLIENT。
  故 DCE 改走新浪逐合约 akshare.futures_zh_daily_sina(symbol="M2409")。
  新浪不给品种/交易所，由合约代码前缀解析；只取【可能成为主力】的月份
  （见 DCE_MONTHS，多数品种为 1/5/9，生猪 1/3/5/7/9/11，PG 全月）。

长跑架构（血泪教训，勿改）
------------------------
每块 = 一个 (market, year) = 【独立子进程】+ 外部硬超时 + 进程组 SIGKILL + 重试 3 次
+ 单块失败不中断整体。原因：交易所/新浪接口在长驻进程里会退化（socket 损坏 → 全线程
futex 死锁 → 既不报错也不退出），必须靠外部杀进程兜底。

用法
----
    # 1) 看有哪些块
    PYTHONPATH=<pond> /usr/bin/python3.12 load_futures_contracts_to_db.py --list

    # 2) 跑单块（父进程就是这么调自己）
    ... --market SHFE --year 2024

    # 3) 全量（分块 + 超时 + 重试），建议用 systemd-run 隔离内存
    systemd-run --user --scope -q -p MemoryMax=4G --collect \
        env PYTHONPATH=<pond> CLICKHOUSE_HOST=192.168.1.200 /usr/bin/python3.12 \
        load_futures_contracts_to_db.py --all --timeout 1800 --retries 3
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import subprocess
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd

# ── 常量 ─────────────────────────────────────────────────────────────────────
# ⚠️ GFEX 不走交易所接口：实测 ak.get_futures_daily(market="GFEX") 严重漏数
#    （2023 年只返回 1~2 月共 20 个交易日就停，2024/2025/2026 返回空），
#    属于【静默丢数】，比报错更危险。故与 DCE 一起走新浪逐合约。
EXCH_MARKETS = ["SHFE", "CZCE", "INE"]              # 走 get_futures_daily
SINA_MARKETS = ["DCE", "GFEX"]                      # 走新浪逐合约
DCE = "DCE"
GFEX = "GFEX"
YEARS = list(range(2015, datetime.now().year + 1))

DCE_VARIETIES = ["A", "B", "C", "CS", "M", "Y", "P", "I", "J", "JM", "JD",
                 "L", "V", "PP", "EG", "EB", "PG", "RR", "LH", "LG"]
# FB(纤维板)/BB(胶合板) 已近乎无成交，不取。
GFEX_VARIETIES = ["SI", "LC", "PS"]

# 可能的【主力月份】。多数品种 1/5/9；生猪单月；PG/GFEX 逐月（合约全月挂牌且主力会移动）。
VAR_MONTHS = {
    "LH": [1, 3, 5, 7, 9, 11],
    "PG": list(range(1, 13)),
    "SI": list(range(1, 13)),
    "LC": list(range(1, 13)),
    "PS": list(range(1, 13)),
}
MONTHS_DEFAULT = [1, 5, 9]

# 品种上市年（只用于跳过明显不可能有数据的年份，省掉大量无效请求；缺省 2015）
LISTING_HINT = {"SI": 2022, "LC": 2023, "PS": 2024, "LG": 2024, "EC": 2023,
                "LU": 2020, "BC": 2020, "NR": 2019, "SC": 2018, "SS": 2019,
                "EG": 2018, "EB": 2019, "PG": 2020, "LH": 2021, "PX": 2023}

# 合约代码形如 RB2405 / M2409 / SC2403（4 位）【以及 CZCE 的 3 位：AP401 / CF405】
CONTRACT_RE = re.compile(r"^([A-Z]{1,2})(\d{3,4})$")

ORM_COLS = ["datetime", "code", "variety", "market", "open", "high", "low",
            "close", "volume", "open_interest", "settle"]


def _log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ── 数据获取 ─────────────────────────────────────────────────────────────────
def _exch_range(market: str, sd: date, ed: date, depth: int = 0,
                errs: list | None = None) -> pd.DataFrame:
    """交易所区间抓取；失败则【二分重试】。

    akshare 的交易所函数会在两种情况下失败，且都要靠二分兜住：
      ① 区间内部分交易日无数据 —— 实测 INE 2017/2018 抛 `_validate_key`
         （INE 是 2018-03 才有原油挂牌，之前整段为空）；
      ② 接口整体抖动 —— 实测 SHFE 2026 连报 3 次 JSONDecodeError、INE 2026 整块失败。
    二分把问题日隔离，坏的部分返回空，好的部分照常拿到。

    ⚠️ `errs` 是【失败计数器】：二分兜底会把「整块全失败」也变成空 df，那样驱动会把它
    当成「合法的空块」跳过 —— 这正是【静默丢数】（实测 INE 2026 就这么丢的）。
    调用方必须用 errs 判断：空结果 + 有异常 ⇒ 抛错让驱动重试。
    """
    import akshare as ak
    if errs is None:
        errs = []
    try:
        df = ak.get_futures_daily(start_date=sd.strftime("%Y%m%d"),
                                  end_date=ed.strftime("%Y%m%d"), market=market)
        return df if df is not None else pd.DataFrame()
    except Exception as e:                                 # noqa: BLE001
        errs.append(f"{type(e).__name__}: {str(e)[:100]}")
        if depth >= 14 or sd >= ed:
            return pd.DataFrame()
        mid = sd + (ed - sd) / 2
        left = _exch_range(market, sd, mid, depth + 1, errs)
        right = _exch_range(market, mid + timedelta(days=1), ed, depth + 1, errs)
        parts = [p for p in (left, right) if len(p)]
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def fetch_exchange_market(market: str, year: int) -> pd.DataFrame:
    """交易所逐日全合约（SHFE/CZCE/INE）。"""
    sd = date(year, 1, 1)
    ed = min(date(year, 12, 31), date.today())
    if sd > date.today():
        return pd.DataFrame()
    errs: list = []
    df = _exch_range(market, sd, ed, 0, errs)
    if len(df) == 0 and errs:
        msg = (f"{market} {year}: 二分后仍全部失败（{len(errs)} 次异常），"
               f"首个异常: {errs[0]} —— 不做静默跳过，交由驱动重试")
        raise RuntimeError(msg)
    if len(df) == 0:
        return pd.DataFrame()
    df = df.copy()
    df["market"] = market
    return df


def fetch_sina_variety_year(market: str, variety: str, year: int) -> pd.DataFrame:
    """新浪逐合约：DCE / GFEX（交易所接口不可用或静默漏数的市场）。

    每个合约一次调用（新浪返回该合约全历史，本地按年截取，重复无害）。
    """
    import akshare as ak
    if year < LISTING_HINT.get(variety, 2015):
        return pd.DataFrame()
    frames = []
    months = VAR_MONTHS.get(variety, MONTHS_DEFAULT)
    for yy in (year % 100, (year + 1) % 100):
        for mm in months:
            sym = f"{variety}{yy:02d}{mm:02d}"
            try:
                d = ak.futures_zh_daily_sina(symbol=sym)
            except Exception:                            # noqa: BLE001  合约不存在 → 跳过
                continue
            if d is None or len(d) == 0:
                continue
            d = d.copy()
            d["code"] = sym
            frames.append(d)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["variety"] = variety
    out["market"] = market
    return out


def _parse_dates(s: pd.Series) -> pd.Series:
    """统一解析各源的 date 列。

    ⚠️ 必须特判数值型：**CZCE 的 date 是 int64**（如 20240102），而
    `pd.to_datetime(20240102)` 会把它当【纳秒】解释 → 全部变成 1970-01-01！
    （实测：整个郑商所 29 个品种的日期全被写成 1970-01-01 ⇒ 数据实际不可用。）
    """
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_datetime(s.astype("int64").astype(str), format="%Y%m%d", errors="coerce")
    return pd.to_datetime(s, errors="coerce")


def to_orm(df: pd.DataFrame, market: str) -> pd.DataFrame:
    """把不同源的 df 统一成 ORM 列（直接用 ORM 列名，避免 comment 映射的歧义）。"""
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=ORM_COLS)
    d = df.copy()
    if "code" not in d.columns and "symbol" in d.columns:
        d = d.rename(columns={"symbol": "code"})        # 交易所源用 symbol 表示合约
    d["datetime"] = _parse_dates(d["date"])
    d["open_interest"] = d["open_interest"] if "open_interest" in d.columns else d["hold"]
    if "variety" not in d.columns or d["variety"].isna().all():
        d["variety"] = d["code"].str.extract(r"^([A-Z]{1,2})")[0]
    d["market"] = market
    # 只保留合法合约代码（滤掉 SC_TAS / efp 之类）
    ok = d["code"].astype(str).str.match(CONTRACT_RE)
    d = d[ok]
    for c in ("open", "high", "low", "close", "volume", "open_interest", "settle"):
        if c not in d.columns:
            d[c] = float("nan")
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["close"])
    # 丢弃无效行：日期解析失败（如 CZCE int 日期被误解析）或收盘价非正
    d = d[d["datetime"].notna() & (d["close"] > 0)]
    d["code"] = d["code"].astype(str)
    d["variety"] = d["variety"].astype(str)
    d["market"] = d["market"].astype(str)
    return d[ORM_COLS].reset_index(drop=True)


# ── 写库 ─────────────────────────────────────────────────────────────────────
def _manager():
    from pond.clickhouse.manager import ClickHouseManager
    host = os.environ.get("CLICKHOUSE_HOST", "192.168.1.200")
    db = os.environ.get("CLICKHOUSE_DB", "quant")
    pwd = os.environ.get("CLICKHOUSE_PWD", "")
    return ClickHouseManager(
        f"clickhouse://default:{pwd}@{host}:8123/{db}",
        data_start=datetime(2015, 1, 1),
        native_uri=f"clickhouse+native://default:{pwd}@{host}:9000/{db}?tcp_keepalive=true",
    )


def run_chunk(market: str, year: int) -> int:
    """跑一个块，返回写入行数。"""
    from pond.clickhouse.kline import CnFuturesContract1d

    t0 = time.time()
    if market in SINA_MARKETS:
        varieties = DCE_VARIETIES if market == DCE else GFEX_VARIETIES
        frames = [fetch_sina_variety_year(market, v, year) for v in varieties]
        frames = [f for f in frames if len(f)]
        raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        raw = fetch_exchange_market(market, year)

    df = to_orm(raw, market)
    _log(f"{market} {year}: 源 {0 if raw is None else len(raw)} 行 → 清洗后 {len(df)} 行 "
         f"({df['code'].nunique() if len(df) else 0} 合约 / {df['variety'].nunique() if len(df) else 0} 品种) "
         f"{time.time()-t0:.1f}s")
    if len(df) == 0:
        _log(f"{market} {year}: 空块，跳过（不视为失败）")
        return 0
    mgr = _manager()
    rows = mgr.save_to_db(CnFuturesContract1d, df, last_record_filters=None, trunk_size=200_000)
    _log(f"{market} {year}: 写入 {rows} 行  {time.time()-t0:.1f}s")
    return rows or 0


# ── 分块驱动 ─────────────────────────────────────────────────────────────────
def chunk_list(only: list[str] | None = None) -> list[tuple[str, int]]:
    """全部块；only 形如 ["CZCE:*", "GFEX:*", "SHFE:2026"] 时只跑选中块（补跑用）。"""
    all_c = [(m, y) for m in EXCH_MARKETS + SINA_MARKETS for y in YEARS]
    if not only:
        return all_c
    sel: list[tuple[str, int]] = []
    for spec in only:
        mk, _, yr = spec.partition(":")
        mk = mk.upper().strip()
        for m, y in all_c:
            if m == mk and (yr.strip() in ("", "*") or int(yr) == y):
                sel.append((m, y))
    return sel


def run_all(timeout: int, retries: int, only: list[str] | None = None) -> int:
    chunks = chunk_list(only)
    _log(f"共 {len(chunks)} 块（{'全部' if not only else only}），"
         f"单块超时 {timeout}s，失败重试 {retries} 次")
    ok, failed = [], []
    for i, (market, year) in enumerate(chunks, 1):
        done = False
        for attempt in range(1, retries + 1):
            t0 = time.time()
            proc = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), "--market", market, "--year", str(year)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                start_new_session=True,                       # 独立进程组，便于整组 SIGKILL
            )
            try:
                out, _ = proc.communicate(timeout=timeout)
                if proc.returncode == 0:
                    _log(f"[{i}/{len(chunks)}] ✅ {market} {year}  "
                         f"尝试 {attempt}/{retries}  {time.time()-t0:.1f}s")
                    ok.append((market, year)); done = True
                    break
                _log(f"[{i}/{len(chunks)}] ⚠️ {market} {year} rc={proc.returncode} "
                     f"尝试 {attempt}/{retries}  输出尾部: {out.strip()[-300:]}")
            except subprocess.TimeoutExpired:
                _log(f"[{i}/{len(chunks)}] ⏱ {market} {year} TIMEOUT {timeout}s "
                     f"（尝试 {attempt}/{retries}）→ 杀进程组")
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:                            # noqa: BLE001
                    proc.kill()
                try:
                    proc.communicate(timeout=30)
                except Exception:                            # noqa: BLE001
                    pass
            if attempt < retries:
                time.sleep(5)
        if not done:
            _log(f"[{i}/{len(chunks)}] ❌ {market} {year} 重试 {retries} 次仍失败 → "
                 f"【不中断整体】，继续下一块")
            failed.append((market, year))
    _log(f"== 完成：成功 {len(ok)}/{len(chunks)}，失败 {len(failed)} ==")
    if failed:
        _log(f"失败块: {failed}")
    return 0 if not failed else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=EXCH_MARKETS + SINA_MARKETS)
    ap.add_argument("--year", type=int)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--chunks", type=str, default="",
                    help='只跑选中块，如 "CZCE:*,GFEX:*,SHFE:2026"（补跑用）')
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--retries", type=int, default=3)
    a = ap.parse_args()

    only = a.chunks.split(",") if a.chunks else None
    if a.list:
        for m, y in chunk_list(only):
            print(f"{m} {y}")
        return 0
    if a.all:
        return run_all(a.timeout, a.retries, only)
    if a.market and a.year:
        run_chunk(a.market, a.year)
        return 0
    ap.error("需要 --all / --list / (--market 且 --year)")


if __name__ == "__main__":
    sys.exit(main())
